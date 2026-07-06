use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::os::unix::io::AsRawFd;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum Predicate {
    Eq(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
    In(Vec<Value>, HashSet<String>),
}

#[derive(Debug, Clone)]
pub enum Filter {
    Cond(String, Predicate),
    And(Vec<Filter>),
    Or(Vec<Filter>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortDir { Asc, Desc }

#[derive(Debug, Clone)]
pub struct SortKey {
    pub field: String,
    pub dir:   SortDir,
}

pub struct Query {
    pub select:     Option<Vec<String>>,
    pub collection: Option<String>,
    pub filter:     Option<Filter>,
    pub order:      Vec<SortKey>,
    pub limit:      Option<usize>,
    pub offset:     usize,
}

impl Query {
    pub fn from_json(raw: &Value) -> Result<Self, String> {
        let select = match raw.get("select") {
            Some(Value::Array(arr)) => {
                let fields: Result<Vec<String>, _> = arr
                    .iter()
                    .map(|v| v.as_str().map(|s| s.to_string()).ok_or("select deve contenere stringhe"))
                    .collect();
                Some(fields?)
            }
            Some(_) => return Err("select deve essere un array di stringhe".to_string()),
            None    => None,
        };

        let filter = match raw.get("where") {
            Some(w) => Some(Self::parse_filter(w)?),
            None    => None,
        };

        let order = match raw.get("order") {
            Some(Value::Array(arr)) => arr.iter().map(|o| {
                let field = o["field"].as_str().ok_or("order.field mancante")?.to_string();
                let dir = match o["dir"].as_str().unwrap_or("asc").to_lowercase().as_str() {
                    "desc" => SortDir::Desc,
                    _      => SortDir::Asc,
                };
                Ok(SortKey { field, dir })
            }).collect::<Result<Vec<_>, String>>()?,
            _ => vec![],
        };

        let limit      = raw.get("limit").and_then(|v| v.as_u64()).map(|n| n as usize);
        let offset     = raw.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
        let collection = raw.get("collection").and_then(|v| v.as_str()).map(|s| s.to_string());

        Ok(Query { select, collection, filter, order, limit, offset })
    }

    fn parse_filter(v: &Value) -> Result<Filter, String> {
        if let Some(arr) = v.get("$and").and_then(|a| a.as_array()) {
            let children: Result<Vec<_>, _> = arr.iter().map(Self::parse_filter).collect();
            return Ok(Filter::And(children?));
        }
        if let Some(arr) = v.get("$or").and_then(|a| a.as_array()) {
            let children: Result<Vec<_>, _> = arr.iter().map(Self::parse_filter).collect();
            return Ok(Filter::Or(children?));
        }
        if let Some(obj) = v.as_object() {
            let mut conds = vec![];
            for (field, cond) in obj {
                conds.push(Filter::Cond(field.clone(), Self::parse_predicate(cond)?));
            }
            return if conds.len() == 1 {
                Ok(conds.remove(0))
            } else {
                Ok(Filter::And(conds))
            };
        }
        Err("Filtro WHERE non valido".to_string())
    }

    fn parse_predicate(v: &Value) -> Result<Predicate, String> {
        if let Some(obj) = v.as_object() {
            if obj.len() != 1 {
                return Err("Il predicato deve avere esattamente un operatore".to_string());
            }
            let (op, val) = obj.iter().next().unwrap();
            return match op.as_str() {
                "$eq"  | "="  | "eq"  => Ok(Predicate::Eq(val.clone())),
                "$gt"  | ">"  | "gt"  => Ok(Predicate::Gt(val.clone())),
                "$gte" | ">=" | "gte" => Ok(Predicate::Gte(val.clone())),
                "$lt"  | "<"  | "lt"  => Ok(Predicate::Lt(val.clone())),
                "$lte" | "<=" | "lte" => Ok(Predicate::Lte(val.clone())),
                "$in" => {
                    let items = val.as_array().ok_or("$in richiede un array")?.clone();
                    let keys: HashSet<String> = items.iter().map(value_key).collect();
                    Ok(Predicate::In(items, keys))
                }
                other => Err(format!("Operatore non supportato: '{}'", other)),
            };
        }
        Ok(Predicate::Eq(v.clone()))
    }
}

type Index = HashMap<String, HashMap<String, HashSet<String>>>;

fn value_key(v: &Value) -> String {
    v.to_string()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Clone, Debug, PartialEq)]
pub enum FsyncMode {
    Always,
    Interval,
    Never,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub port:                 u16,
    pub role:                 String,
    pub leader_url:           String,
    pub sync_interval_secs:   u64,
    pub autoload_path:        Option<String>,
    pub segment_dir:          String,
    pub segment_max_mb:       u64,
    pub compact_wal_count:    usize,
    pub compact_wal_mb:       u64,
    pub fsync_mode:           FsyncMode,
    pub fsync_interval_ms:    u64,
    pub shard_key:            Option<String>,
    pub shard_index:          u64,
    pub shard_total:          u64,
}

impl Config {
    pub fn from_env() -> Self {
        fn env(key: &str, default: &str) -> String {
            std::env::var(key).unwrap_or_else(|_| default.to_string())
        }
        fn env_u64(key: &str, default: u64) -> u64 {
            std::env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
        }
        let autoload_path = match env("AUTOLOAD_PATH", "").as_str() {
            "" => None,
            p  => Some(p.to_string()),
        };
        let shard_key = match env("SHARD_KEY", "").as_str() {
            "" => None,
            k  => Some(k.to_string()),
        };
        let fsync_mode = match env("FSYNC", "interval").to_lowercase().as_str() {
            "always" => FsyncMode::Always,
            "never"  => FsyncMode::Never,
            _        => FsyncMode::Interval,
        };
        Config {
            port:               env_u64("PORT", 3000) as u16,
            role:               env("ROLE", "leader"),
            leader_url:         env("LEADER_URL", "http://leader:3000"),
            sync_interval_secs: env_u64("SYNC_INTERVAL_SECS", 5),
            autoload_path,
            segment_dir:        env("SEGMENT_DIR", "./segments"),
            segment_max_mb:     env_u64("SEGMENT_MAX_MB", 64),
            compact_wal_count:  env_u64("COMPACT_WAL_COUNT", 8) as usize,
            compact_wal_mb:     env_u64("COMPACT_WAL_MB", 256),
            fsync_mode,
            fsync_interval_ms:  env_u64("FSYNC_INTERVAL_MS", 200),
            shard_key,
            shard_index:        env_u64("SHARD_INDEX", 0),
            shard_total:        env_u64("SHARD_TOTAL", 1),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct Snapshot {
    ts:          u64,
    data:        HashMap<String, Value>,
    index:       HashMap<String, HashMap<String, HashSet<String>>>,
    collections: HashMap<String, HashSet<String>>,
}

pub struct Aledb {
    pub config:       Config,
    data:             HashMap<String, Value>,
    index:            Index,
    modified_at:      HashMap<String, u64>,
    modified_ts:      BTreeMap<u64, HashSet<String>>,
    collections:      HashMap<String, HashSet<String>>,
    current_wal:      Option<File>,
    current_wal_path: Option<String>,
    current_wal_size: u64,
    wal_paths:        Vec<String>,
    pending_fsync:    bool,
}

impl Aledb {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            data:             HashMap::new(),
            index:            HashMap::new(),
            modified_at:      HashMap::new(),
            modified_ts:      BTreeMap::new(),
            collections:      HashMap::new(),
            current_wal:      None,
            current_wal_path: None,
            current_wal_size: 0,
            wal_paths:        Vec::new(),
            pending_fsync:    false,
        }
    }

    pub fn autoload(&mut self) {
        fs::create_dir_all(&self.config.segment_dir).ok();

        let snap_path = self.latest_file("snap");
        let snap_ts = snap_path.as_deref()
            .and_then(|p| self.ts_from_path(p))
            .unwrap_or(0);

        if let Some(ref path) = snap_path {
            match self.load_snapshot(path) {
                Ok(_)  => println!("[DB] snapshot loaded from {}", path),
                Err(e) => eprintln!("[DB] snapshot load failed: {}", e),
            }
        } else if let Some(path) = self.config.autoload_path.clone() {
            if std::path::Path::new(&path).exists() {
                match self.load(&path) {
                    Ok(_)  => println!("[DB] init loaded from {}", path),
                    Err(e) => eprintln!("[DB] init load failed: {}", e),
                }
            }
        }

        let wals = self.sorted_files_since("wal", snap_ts);
        self.wal_paths = wals.clone();
        for path in &wals {
            if let Err(e) = self.replay_wal(path) {
                eprintln!("[DB] WAL replay error {}: {}", path, e);
            }
        }

        self.rotate_wal();
    }

    fn load_snapshot(&mut self, path: &str) -> Result<(), String> {
        let bytes = fs::read(path).map_err(|e| e.to_string())?;
        let snap: Snapshot = rmp_serde::from_slice(&bytes).map_err(|e| e.to_string())?;
        self.data        = snap.data;
        self.index       = snap.index;
        self.collections = snap.collections;
        let ts = snap.ts;
        let mut ts_set = HashSet::with_capacity(self.data.len());
        self.modified_at = self.data.keys().map(|id| {
            ts_set.insert(id.clone());
            (id.clone(), ts)
        }).collect();
        self.modified_ts.insert(ts, ts_set);
        Ok(())
    }

    fn replay_wal(&mut self, path: &str) -> Result<(), String> {
        let file = File::open(path).map_err(|e| e.to_string())?;
        let mut reader = BufReader::new(file);
        let mut len_buf = [0u8; 4];

        loop {
            if reader.read_exact(&mut len_buf).is_err() { break; }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() { break; }

            let op: Value = match rmp_serde::from_slice(&payload) {
                Ok(v)  => v,
                Err(_) => continue,
            };

            match op["op"].as_str() {
                Some("insert") => {
                    if let Some(doc) = op.get("doc") {
                        let id = doc["id"].as_str().ok_or("missing id")?.to_string();
                        self.index_doc(&id, doc);
                        self.set_modified(&id, now_ms());
                        if let Some(coll) = op["collection"].as_str() {
                            self.collections.entry(coll.to_string()).or_default().insert(id.clone());
                        }
                        self.data.insert(id, doc.clone());
                    }
                }
                Some("patch") => {
                    if let (Some(id), Some(fields)) = (op["id"].as_str(), op.get("fields")) {
                        let id = id.to_string();
                        if let Some(mut doc) = self.data.remove(&id) {
                            self.deindex_doc(&id, &doc);
                            if let (Value::Object(d), Value::Object(f)) = (&mut doc, fields.clone()) {
                                for (k, v) in f { d.insert(k, v); }
                            }
                            self.index_doc(&id, &doc);
                            self.set_modified(&id, now_ms());
                            self.data.insert(id, doc);
                        }
                    }
                }
                Some("delete") => {
                    if let Some(id) = op["id"].as_str() {
                        let id = id.to_string();
                        if let Some(doc) = self.data.remove(&id) {
                            self.deindex_doc(&id, &doc);
                            self.unset_modified(&id);
                        }
                        for ids in self.collections.values_mut() {
                            ids.remove(&id);
                        }
                    }
                }
                _ => {}
            }
        }
        println!("[DB] WAL replayed {}", path);
        Ok(())
    }

    fn rotate_wal(&mut self) {
        let path = format!("{}/wal_{}.msgpack", self.config.segment_dir, now_ms());
        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => {
                self.current_wal      = Some(file);
                self.current_wal_path = Some(path.clone());
                self.current_wal_size = 0;
                self.wal_paths.push(path);
            }
            Err(e) => eprintln!("[DB] failed to open WAL: {}", e),
        }
    }

    fn write_wal_record(&mut self, record: &Value) {
        let payload = match rmp_serde::to_vec(record) {
            Ok(p)  => p,
            Err(_) => return,
        };
        let len = (payload.len() as u32).to_le_bytes();

        let record_size = (4 + payload.len()) as u64;
        if let Some(file) = &mut self.current_wal {
            file.write_all(&len).ok();
            file.write_all(&payload).ok();
            match self.config.fsync_mode {
                FsyncMode::Always   => unsafe { libc_fsync(file.as_raw_fd()); },
                FsyncMode::Interval => { self.pending_fsync = true; }
                FsyncMode::Never    => {}
            }
        }
        self.current_wal_size += record_size;
        if self.current_wal_size > self.config.segment_max_mb * 1024 * 1024 {
            self.rotate_wal();
        }
    }

    pub fn compact(&mut self) -> Result<(), String> {
        let wal_count = self.wal_paths.len();
        let wal_mb: u64 = self.wal_paths.iter()
            .filter_map(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .sum::<u64>() / (1024 * 1024);

        if wal_count < self.config.compact_wal_count && wal_mb < self.config.compact_wal_mb {
            return Ok(());
        }

        println!("[DB] compacting ({} WAL, {} MB)...", wal_count, wal_mb);

        let snap_ts   = now_ms();
        let snap_path = format!("{}/snap_{}.msgpack", self.config.segment_dir, snap_ts);
        let snap = Snapshot {
            ts:          snap_ts,
            data:        self.data.clone(),
            index:       self.index.clone(),
            collections: self.collections.clone(),
        };
        let bytes = rmp_serde::to_vec(&snap).map_err(|e| e.to_string())?;
        fs::write(&snap_path, bytes).map_err(|e| e.to_string())?;

        for p in self.sorted_files_since("snap", 0) {
            if p != snap_path { fs::remove_file(&p).ok(); }
        }
        for p in self.wal_paths.drain(..) {
            fs::remove_file(&p).ok();
        }

        self.rotate_wal();
        println!("[DB] compact done → {}", snap_path);
        Ok(())
    }

    pub fn flush_fsync(&mut self) {
        if self.pending_fsync {
            if let Some(file) = &self.current_wal {
                unsafe { libc_fsync(file.as_raw_fd()); }
            }
            self.pending_fsync = false;
        }
    }

    fn latest_file(&self, prefix: &str) -> Option<String> {
        self.sorted_files_since(prefix, 0).into_iter().last()
    }

    fn sorted_files_since(&self, prefix: &str, since_ts: u64) -> Vec<String> {
        let Ok(entries) = fs::read_dir(&self.config.segment_dir) else { return vec![]; };
        let mut pairs: Vec<(u64, String)> = entries
            .filter_map(|e| {
                let path = e.ok()?.path();
                let name = path.file_name()?.to_str()?;
                if !name.starts_with(prefix) || path.extension()?.to_str()? != "msgpack" {
                    return None;
                }
                let stem    = path.file_stem()?.to_str()?;
                let ts_part = stem.strip_prefix(prefix)
                    .and_then(|s| s.strip_prefix('_'))
                    .unwrap_or(stem);
                let ts: u64 = ts_part.parse().ok()?;
                if ts > since_ts { Some((ts, path.to_string_lossy().to_string())) } else { None }
            })
            .collect();
        pairs.sort_by_key(|(ts, _)| *ts);
        pairs.into_iter().map(|(_, p)| p).collect()
    }

    fn ts_from_path(&self, path: &str) -> Option<u64> {
        let stem = std::path::Path::new(path).file_stem()?.to_str()?.to_string();
        stem.rsplit('_').next()?.parse().ok()
    }

    pub fn owns_doc(&self, doc: &Value) -> bool {
        if self.config.shard_total <= 1 { return true; }
        let key = match &self.config.shard_key {
            Some(k) => k,
            None    => return true,
        };
        let val = match doc.get(key) {
            Some(v) => v.to_string(),
            None    => return true,
        };
        (fnv1a(&val) % self.config.shard_total) == self.config.shard_index
    }

    pub fn insert(&mut self, mut doc: Value, collection: Option<&str>) -> Result<String, String> {
        if !doc.is_object() {
            return Err("Il documento deve essere un oggetto JSON".to_string());
        }
        let id = Uuid::new_v4().to_string();
        doc["id"] = Value::String(id.clone());
        self.index_doc(&id, &doc);
        self.set_modified(&id, now_ms());
        let wal_record = match collection {
            Some(c) => {
                self.collections.entry(c.to_string()).or_default().insert(id.clone());
                serde_json::json!({ "op": "insert", "collection": c, "doc": &doc })
            }
            None => serde_json::json!({ "op": "insert", "doc": &doc }),
        };
        self.write_wal_record(&wal_record);
        self.data.insert(id.clone(), doc);
        Ok(id)
    }

    pub fn get_id(&self, id: &str) -> Option<Value> {
        self.data.get(id).cloned()
    }

    pub fn delete(&mut self, id: &str) {
        if let Some(doc) = self.data.remove(id) {
            self.deindex_doc(id, &doc);
            self.unset_modified(id);
            for set in self.collections.values_mut() {
                set.remove(id);
            }
            self.write_wal_record(&serde_json::json!({ "op": "delete", "id": id }));
        }
    }

    pub fn update(&mut self, id: &str, patch: Value) {
        if let Some(old_doc) = self.data.get(id) {
            self.deindex_doc(id, old_doc);
        }
        self.write_wal_record(&serde_json::json!({ "op": "patch", "id": id, "fields": &patch }));
        if let Some(existing) = self.data.get_mut(id) {
            if let (Value::Object(e), Value::Object(p)) = (existing, patch) {
                for (k, v) in p { e.insert(k, v); }
            }
        }
        if let Some(updated_doc) = self.data.get(id) {
            self.index_doc(id, updated_doc);
            self.set_modified(id, now_ms());
        }
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        let docs: Vec<&Value> = self.data.values().collect();
        let json = serde_json::to_string_pretty(&docs).map_err(|e| e.to_string())?;
        fs::write(path, json).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn load(&mut self, path: &str) -> Result<(), String> {
        let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let docs: Vec<Value> = serde_json::from_str(&content).map_err(|e| e.to_string())?;
        for doc in docs {
            let id = doc["id"].as_str().ok_or("Missing id")?.to_string();
            self.index_doc(&id, &doc);
            self.set_modified(&id, now_ms());
            self.data.insert(id, doc);
        }
        Ok(())
    }

    pub fn docs_since(&self, since_ms: u64) -> Vec<Value> {
        use std::ops::Bound;
        self.modified_ts
            .range((Bound::Excluded(since_ms), Bound::Unbounded))
            .flat_map(|(_, ids)| ids.iter())
            .filter_map(|id| self.data.get(id).cloned())
            .collect()
    }

    pub fn apply_docs(&mut self, docs: Vec<Value>, leader_ts: u64) {
        for doc in docs {
            let id = match doc["id"].as_str() {
                Some(id) => id.to_string(),
                None     => continue,
            };
            if let Some(old) = self.data.remove(&id) {
                self.deindex_doc(&id, &old);
            }
            self.index_doc(&id, &doc);
            self.set_modified(&id, leader_ts);
            self.data.insert(id, doc);
        }
    }

    pub fn list_collections(&self) -> Vec<(String, usize)> {
        self.collections
            .iter()
            .map(|(name, ids)| (name.clone(), ids.len()))
            .collect()
    }

    pub fn docs_for_tenant(&self, shard_key: &str, tenant_id: &str) -> Vec<Value> {
        let key = value_key(&serde_json::Value::String(tenant_id.to_string()));
        self.index
            .get(shard_key)
            .and_then(|m| m.get(&key))
            .map(|ids| ids.iter().filter_map(|id| self.data.get(id).cloned()).collect())
            .unwrap_or_default()
    }

    pub fn delete_tenant(&mut self, shard_key: &str, tenant_id: &str) {
        let key   = value_key(&serde_json::Value::String(tenant_id.to_string()));
        let ids: Vec<String> = self.index
            .get(shard_key)
            .and_then(|m| m.get(&key))
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default();
        for id in ids {
            if let Some(doc) = self.data.remove(&id) {
                self.deindex_doc(&id, &doc);
                self.unset_modified(&id);
                for set in self.collections.values_mut() {
                    set.remove(&id);
                }
                self.write_wal_record(&serde_json::json!({ "op": "delete", "id": &id }));
            }
        }
    }

    pub fn query(&self, q: &Query) -> Vec<Value> {
        let collection_ids: Option<&HashSet<String>> = q.collection
            .as_deref()
            .and_then(|c| self.collections.get(c));

        let mut results: Vec<Value> = if let Some(filter) = &q.filter {
            let base: Vec<&Value> = match collection_ids {
                Some(ids) => ids.iter().filter_map(|id| self.data.get(id)).collect(),
                None => match self.index_hint(filter) {
                    Some(hint_ids) => hint_ids.iter().filter_map(|id| self.data.get(id)).collect(),
                    None           => self.data.values().collect(),
                },
            };
            base.into_iter()
                .filter(|doc| Self::eval_filter(doc, filter))
                .map(|doc| Self::project(doc, &q.select))
                .collect()
        } else {
            match collection_ids {
                Some(ids) => ids.iter()
                    .filter_map(|id| self.data.get(id))
                    .map(|doc| Self::project(doc, &q.select))
                    .collect(),
                None => self.data.values()
                    .map(|doc| Self::project(doc, &q.select))
                    .collect(),
            }
        };

        if !q.order.is_empty() {
            results.sort_by(|a, b| {
                for key in &q.order {
                    let ord = Self::cmp_values(a.get(&key.field), b.get(&key.field));
                    let ord = if key.dir == SortDir::Desc { ord.reverse() } else { ord };
                    if ord != std::cmp::Ordering::Equal { return ord; }
                }
                std::cmp::Ordering::Equal
            });
        }

        let start = q.offset.min(results.len());
        let it = results.into_iter().skip(start);
        match q.limit {
            Some(n) => it.take(n).collect(),
            None    => it.collect(),
        }
    }

    fn index_hint(&self, filter: &Filter) -> Option<Vec<String>> {
        match filter {
            Filter::Cond(field, Predicate::Eq(val)) => {
                self.index
                    .get(field.as_str())
                    .and_then(|m| m.get(&value_key(val)))
                    .map(|s| s.iter().cloned().collect())
            }
            Filter::And(children) => {
                let sets: Vec<HashSet<&str>> = children.iter()
                    .filter_map(|c| match c {
                        Filter::Cond(field, Predicate::Eq(val)) => {
                            self.index
                                .get(field.as_str())
                                .and_then(|m| m.get(&value_key(val)))
                                .map(|s| s.iter().map(|id| id.as_str()).collect::<HashSet<_>>())
                        }
                        _ => None,
                    })
                    .collect();
                if sets.is_empty() {
                    return None;
                }
                let smallest = sets.iter().min_by_key(|s| s.len())?;
                let result: Vec<String> = smallest.iter()
                    .filter(|id| sets.iter().all(|s| s.contains(*id)))
                    .map(|s| s.to_string())
                    .collect();
                Some(result)
            }
            _ => None,
        }
    }

    fn eval_filter(doc: &Value, filter: &Filter) -> bool {
        match filter {
            Filter::Cond(field, pred) => doc.get(field).map(|v| Self::eval_pred(v, pred)).unwrap_or(false),
            Filter::And(children)     => children.iter().all(|f| Self::eval_filter(doc, f)),
            Filter::Or(children)      => children.iter().any(|f| Self::eval_filter(doc, f)),
        }
    }

    fn eval_pred(val: &Value, pred: &Predicate) -> bool {
        match pred {
            Predicate::Eq(e)   => val == e,
            Predicate::Gt(t)   => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a > b),
            Predicate::Gte(t)  => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a >= b),
            Predicate::Lt(t)   => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a < b),
            Predicate::Lte(t)  => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a <= b),
            Predicate::In(_, keys) => keys.contains(&value_key(val)),
        }
    }

    fn cmp_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
        match (a, b) {
            (None, None)       => std::cmp::Ordering::Equal,
            (None, _)          => std::cmp::Ordering::Greater,
            (_, None)          => std::cmp::Ordering::Less,
            (Some(x), Some(y)) => {
                if let (Some(xf), Some(yf)) = (x.as_f64(), y.as_f64()) {
                    xf.partial_cmp(&yf).unwrap_or(std::cmp::Ordering::Equal)
                } else if let (Some(xs), Some(ys)) = (x.as_str(), y.as_str()) {
                    xs.cmp(ys)
                } else {
                    x.to_string().cmp(&y.to_string())
                }
            }
        }
    }

    fn set_modified(&mut self, id: &str, ts: u64) {
        if let Some(old_ts) = self.modified_at.get(id).copied() {
            if let Some(set) = self.modified_ts.get_mut(&old_ts) {
                set.remove(id);
                if set.is_empty() {
                    self.modified_ts.remove(&old_ts);
                }
            }
        }
        self.modified_at.insert(id.to_string(), ts);
        self.modified_ts.entry(ts).or_default().insert(id.to_string());
    }

    fn unset_modified(&mut self, id: &str) {
        if let Some(ts) = self.modified_at.remove(id) {
            if let Some(set) = self.modified_ts.get_mut(&ts) {
                set.remove(id);
                if set.is_empty() {
                    self.modified_ts.remove(&ts);
                }
            }
        }
    }

    fn index_doc(&mut self, id: &str, doc: &Value) {
        let Some(obj) = doc.as_object() else { return };
        let id_owned = id.to_string();
        for (field, val) in obj {
            self.index
                .entry(field.clone())
                .or_default()
                .entry(value_key(val))
                .or_default()
                .insert(id_owned.clone());
        }
    }

    fn deindex_doc(&mut self, id: &str, doc: &Value) {
        let Some(obj) = doc.as_object() else { return };
        for (field, val) in obj {
            let key = value_key(val);
            if let Some(field_map) = self.index.get_mut(field) {
                if let Some(ids) = field_map.get_mut(&key) {
                    ids.remove(id);
                    if ids.is_empty() {
                        field_map.remove(&key);
                    }
                }
            }
        }
    }

    fn project(doc: &Value, select: &Option<Vec<String>>) -> Value {
        match select {
            None => doc.clone(),
            Some(fields) => {
                let mut out = serde_json::Map::with_capacity(fields.len());
                for field in fields {
                    if let Some(v) = doc.get(field) {
                        out.insert(field.clone(), v.clone());
                    }
                }
                Value::Object(out)
            }
        }
    }
}

unsafe fn libc_fsync(fd: i32) {
    extern "C" { fn fsync(fd: i32) -> i32; }
    fsync(fd);
}

fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 14695981039346656037;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash  = hash.wrapping_mul(1099511628211);
    }
    hash
}