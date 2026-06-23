use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::os::unix::io::AsRawFd;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug)]
pub enum Predicate {
    Eq(Value),
    Gt(Value),
    Gte(Value),
    Lt(Value),
    Lte(Value),
}

pub struct Query {
    pub select: Option<Vec<String>>,
    pub filters: Vec<(String, Predicate)>,
}

impl Query {
    pub fn from_json(raw: &Value) -> Result<Self, String> {
        let select = match raw.get("select") {
            Some(Value::Array(arr)) => {
                let fields: Result<Vec<String>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or("I campi in 'select' devono essere stringhe")
                    })
                    .collect();
                Some(fields?)
            }
            Some(_) => return Err("'select' deve essere un array di stringhe".to_string()),
            None => None,
        };

        let mut filters = Vec::new();

        if let Some(Value::Object(where_map)) = raw.get("where") {
            for (field, condition) in where_map {
                let predicate = if condition.is_object() {
                    let obj = condition.as_object().unwrap();
                    if obj.len() != 1 {
                        return Err(format!(
                            "Il campo '{}' deve avere esattamente un operatore",
                            field
                        ));
                    }
                    let (op, val) = obj.iter().next().unwrap();
                    match op.as_str() {
                        "="  | "eq"  => Predicate::Eq(val.clone()),
                        ">"  | "gt"  => Predicate::Gt(val.clone()),
                        ">=" | "gte" => Predicate::Gte(val.clone()),
                        "<"  | "lt"  => Predicate::Lt(val.clone()),
                        "<=" | "lte" => Predicate::Lte(val.clone()),
                        other => return Err(format!("Operatore non supportato: '{}'", other)),
                    }
                } else {
                    Predicate::Eq(condition.clone())
                };

                filters.push((field.clone(), predicate));
            }
        }

        Ok(Query { select, filters })
    }
}

type Index = HashMap<String, HashMap<String, Vec<String>>>;

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
            "always"   => FsyncMode::Always,
            "never"    => FsyncMode::Never,
            _          => FsyncMode::Interval,
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

// DB engine

#[derive(Serialize, Deserialize)]
struct Snapshot {
    ts:   u64,
    data: HashMap<String, Value>,
    index: HashMap<String, HashMap<String, Vec<String>>>,
}

pub struct Aledb {
    pub config:           Config,
    data:                 HashMap<String, Value>,
    index:                Index,
    modified_at:          HashMap<String, u64>,
    current_wal:          Option<File>,
    current_wal_path:     Option<String>,
    wal_paths:            Vec<String>,
    pending_fsync:        bool,
}

impl Aledb {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            data:             HashMap::new(),
            index:            HashMap::new(),
            modified_at:      HashMap::new(),
            current_wal:      None,
            current_wal_path: None,
            wal_paths:        Vec::new(),
            pending_fsync:    false,
        }
    }

    pub fn autoload(&mut self) {
        fs::create_dir_all(&self.config.segment_dir).ok();

        // 1. Loads snapshot
        if let Some(snap_path) = self.latest_file("snap") {
            match self.load_snapshot(&snap_path) {
                Ok(_)  => println!("[DB] snapshot loaded from {}", snap_path),
                Err(e) => eprintln!("[DB] snapshot load failed: {}", e),
            }
        } else if let Some(path) = self.config.autoload_path.clone() {
            // If no snapshot: loads init.json
            if std::path::Path::new(&path).exists() {
                match self.load(&path) {
                    Ok(_)  => println!("[DB] init loaded from {}", path),
                    Err(e) => eprintln!("[DB] init load failed: {}", e),
                }
            }
        }

        // 2. WAL replays after snapshot (recent changes only)
        let snap_ts = self.latest_file("snap")
            .and_then(|p| self.ts_from_path(&p))
            .unwrap_or(0);

        let wals = self.sorted_files_since("wal", snap_ts);
        self.wal_paths = wals.clone();
        for path in &wals {
            if let Err(e) = self.replay_wal(path) {
                eprintln!("[DB] WAL replay error {}: {}", path, e);
            }
        }

        // 3. Opens a new WAL for current
        self.rotate_wal();
    }

    fn load_snapshot(&mut self, path: &str) -> Result<(), String> {
        let bytes = fs::read(path).map_err(|e| e.to_string())?;
        let snap: Snapshot = rmp_serde::from_slice(&bytes).map_err(|e| e.to_string())?;
        self.data  = snap.data;
        self.index = snap.index;
        for id in self.data.keys() {
            self.modified_at.insert(id.clone(), snap.ts);
        }
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
                        self.modified_at.insert(id.clone(), now_ms());
                        self.data.insert(id, doc.clone());
                    }
                }
                // Delta update
                Some("patch") => {
                    if let (Some(id), Some(fields)) = (op["id"].as_str(), op.get("fields")) {
                        let id = id.to_string();
                        if let Some(old) = self.data.get(&id).cloned() {
                            self.deindex_doc(&id, &old);
                        }
                        if let Some(doc) = self.data.get_mut(&id) {
                            if let (Value::Object(d), Value::Object(f)) = (doc, fields.clone()) {
                                for (k, v) in f { d.insert(k, v); }
                            }
                        }
                        if let Some(updated) = self.data.get(&id).cloned() {
                            self.index_doc(&id, &updated);
                            self.modified_at.insert(id, now_ms());
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

        if let Some(file) = &mut self.current_wal {
            file.write_all(&len).ok();
            file.write_all(&payload).ok();

            match self.config.fsync_mode {
                FsyncMode::Always => {
                    unsafe { libc_fsync(file.as_raw_fd()); }
                }
                FsyncMode::Interval => {
                    self.pending_fsync = true;
                }
                FsyncMode::Never => {}
            }
        }

        let too_big = self.current_wal_path.as_ref()
            .and_then(|p| fs::metadata(p).ok())
            .map(|m| m.len() > self.config.segment_max_mb * 1024 * 1024)
            .unwrap_or(false);
        if too_big {
            self.rotate_wal();
        }
    }

    // Called by compaction loop in main.rs
    pub fn compact(&mut self) -> Result<(), String> {
        let wal_count = self.wal_paths.len();
        let wal_mb: u64 = self.wal_paths.iter()
            .filter_map(|p| fs::metadata(p).ok())
            .map(|m| m.len())
            .sum::<u64>() / (1024 * 1024);

        let should = wal_count >= self.config.compact_wal_count
            || wal_mb >= self.config.compact_wal_mb;

        if !should {
            return Ok(());
        }

        println!("[DB] compacting ({} WAL, {} MB)...", wal_count, wal_mb);

        let snap_path = format!("{}/snap_{}.msgpack", self.config.segment_dir, now_ms());
        let snap = Snapshot {
            ts:    now_ms(),
            data:  self.data.clone(),
            index: self.index.clone(),
        };
        let bytes = rmp_serde::to_vec(&snap).map_err(|e| e.to_string())?;
        fs::write(&snap_path, bytes).map_err(|e| e.to_string())?;

        // Elimina snapshot e WAL vecchi
        let old_snaps = self.sorted_files_since("snap", 0);
        for p in &old_snaps {
            if p != &snap_path { fs::remove_file(p).ok(); }
        }
        for p in &self.wal_paths.clone() {
            fs::remove_file(p).ok();
        }
        self.wal_paths.clear();

        // Opens new WAL
        self.rotate_wal();
        println!("[DB] compact done → {}", snap_path);
        Ok(())
    }

    // periodic fsync
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
        let Ok(entries) = fs::read_dir(&self.config.segment_dir) else { return vec![] };
        let mut pairs: Vec<(u64, String)> = entries
            .filter_map(|e| {
                let path = e.ok()?.path();
                let name = path.file_name()?.to_str()?.to_string();
                if !name.starts_with(prefix) || path.extension()?.to_str()? != "msgpack" {
                    return None;
                }
                let stem = path.file_stem()?.to_str()?.to_string();
                let ts_str = stem.trim_start_matches(&format!("{}_", prefix));
                let ts: u64 = ts_str.parse().ok()?;
                if ts > since_ts {
                    Some((ts, path.to_string_lossy().to_string()))
                } else {
                    None
                }
            })
            .collect();
        pairs.sort_by_key(|(ts, _)| *ts);
        pairs.into_iter().map(|(_, p)| p).collect()
    }

    fn ts_from_path(&self, path: &str) -> Option<u64> {
        let stem = std::path::Path::new(path).file_stem()?.to_str()?.to_string();
        stem.rsplit('_').next()?.parse().ok()
    }

    // Sharding
    pub fn owns_doc(&self, doc: &Value) -> bool {
        if self.config.shard_total <= 1 {
            return true;
        }
        let key = match &self.config.shard_key {
            Some(k) => k,
            None    => return true,
        };
        let val = match doc.get(key) {
            Some(v) => v.to_string(),
            None    => return true,
        };
        let hash = fnv1a(&val);
        (hash % self.config.shard_total) == self.config.shard_index
    }

    pub fn insert(&mut self, mut doc: Value) -> Result<String, String> {
        if !doc.is_object() {
            return Err("Il documento deve essere un oggetto JSON".to_string());
        }
        let id = Uuid::new_v4().to_string();
        doc["id"] = Value::String(id.clone());
        self.index_doc(&id, &doc);
        self.modified_at.insert(id.clone(), now_ms());
        self.write_wal_record(&serde_json::json!({ "op": "insert", "doc": &doc }));
        self.data.insert(id.clone(), doc);
        Ok(id)
    }

    pub fn get_id(&self, id: &str) -> Option<Value> {
        self.data.get(id).cloned()
    }

    pub fn update(&mut self, id: &str, patch: Value) {
        if let Some(old_doc) = self.data.get(id).cloned() {
            self.deindex_doc(id, &old_doc);
        }
        // Only writes delta
        self.write_wal_record(&serde_json::json!({ "op": "patch", "id": id, "fields": &patch }));
        if let Some(existing) = self.data.get_mut(id) {
            if let (Value::Object(e), Value::Object(p)) = (existing, patch) {
                for (k, v) in p { e.insert(k, v); }
            }
        }
        if let Some(updated_doc) = self.data.get(id).cloned() {
            self.index_doc(id, &updated_doc);
            self.modified_at.insert(id.to_string(), now_ms());
        }
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        let docs: Vec<&Value> = self.data.values().collect();
        let json = serde_json::to_string_pretty(&docs).map_err(|e| e.to_string())?;
        fs::write(path, json).map_err(|e| e.to_string())?;
        Ok(())
    }

    // Loads documents from files, adding them to those already in memory.
    // Existing documents with the same ID are overwritten.
    pub fn load(&mut self, path: &str) -> Result<(), String> {
        let content = fs::read_to_string(path).map_err(|e| e.to_string())?;
        let docs: Vec<Value> = serde_json::from_str(&content).map_err(|e| e.to_string())?;
        for doc in docs {
            let id = doc["id"].as_str().ok_or("Missing id")?.to_string();
            self.index_doc(&id, &doc);
            self.modified_at.insert(id.clone(), now_ms());
            self.data.insert(id, doc);
        }
        Ok(())
    }

    pub fn docs_since(&self, since_ms: u64) -> Vec<Value> {
        self.modified_at
            .iter()
            .filter(|(_, &ts)| ts > since_ms)
            .filter_map(|(id, _)| self.data.get(id).cloned())
            .collect()
    }

    pub fn apply_docs(&mut self, docs: Vec<Value>, leader_ts: u64) {
        for doc in docs {
            let id = match doc["id"].as_str() {
                Some(id) => id.to_string(),
                None => continue,
            };
            if let Some(old) = self.data.get(&id).cloned() {
                self.deindex_doc(&id, &old);
            }
            self.index_doc(&id, &doc);
            self.modified_at.insert(id.clone(), leader_ts);
            self.data.insert(id, doc);
        }
    }

    pub fn query(&self, q: &Query) -> Vec<Value> {
        let (indexed_eq, rest): (Vec<_>, Vec<_>) =
            q.filters.iter().partition(|(field, pred)| {
                matches!(pred, Predicate::Eq(_)) && self.index.contains_key(field.as_str())
            });

        if indexed_eq.is_empty() {
            return self.full_scan(&q.filters, &q.select);
        }

        let best = indexed_eq.iter().min_by_key(|(field, pred)| {
            if let Predicate::Eq(val) = pred {
                self.index
                    .get(field.as_str())
                    .and_then(|m| m.get(&value_key(val)))
                    .map(|ids| ids.len())
                    .unwrap_or(0)
            } else {
                usize::MAX
            }
        });

        let (best_field, best_pred) = best.unwrap();
        let Predicate::Eq(best_val) = best_pred else { unreachable!() };

        let candidates: Vec<&Value> = self
            .index
            .get(best_field.as_str())
            .and_then(|m| m.get(&value_key(best_val)))
            .map(|ids| ids.iter().filter_map(|id| self.data.get(id)).collect())
            .unwrap_or_default();

        let remaining: Vec<(String, Predicate)> = rest
            .iter()
            .chain(indexed_eq.iter().filter(|(f, _)| f != best_field))
            .map(|(f, p)| (f.clone(), match p {
                Predicate::Eq(v)  => Predicate::Eq(v.clone()),
                Predicate::Gt(v)  => Predicate::Gt(v.clone()),
                Predicate::Gte(v) => Predicate::Gte(v.clone()),
                Predicate::Lt(v)  => Predicate::Lt(v.clone()),
                Predicate::Lte(v) => Predicate::Lte(v.clone()),
            }))
            .collect();

        candidates
            .into_iter()
            .filter(|doc| Self::matches(doc, &remaining))
            .map(|doc| Self::project(doc, &q.select))
            .collect()
    }

    fn full_scan(&self, filters: &[(String, Predicate)], select: &Option<Vec<String>>) -> Vec<Value> {
        self.data
            .values()
            .filter(|doc| Self::matches(doc, filters))
            .map(|doc| Self::project(doc, select))
            .collect()
    }

    fn index_doc(&mut self, id: &str, doc: &Value) {
        if let Some(obj) = doc.as_object() {
            for (field, val) in obj {
                self.index
                    .entry(field.clone())
                    .or_default()
                    .entry(value_key(val))
                    .or_default()
                    .push(id.to_string());
            }
        }
    }

    fn deindex_doc(&mut self, id: &str, doc: &Value) {
        if let Some(obj) = doc.as_object() {
            for (field, val) in obj {
                if let Some(field_map) = self.index.get_mut(field) {
                    if let Some(ids) = field_map.get_mut(&value_key(val)) {
                        ids.retain(|x| x != id);
                    }
                }
            }
        }
    }

    fn matches(doc: &Value, filters: &[(String, Predicate)]) -> bool {
        filters.iter().all(|(field, pred)| {
            let Some(field_val) = doc.get(field) else { return false };
            Self::eval(field_val, pred)
        })
    }

    fn eval(val: &Value, pred: &Predicate) -> bool {
        match pred {
            Predicate::Eq(expected) => val == expected,
            Predicate::Gt(t)  => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a > b),
            Predicate::Gte(t) => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a >= b),
            Predicate::Lt(t)  => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a < b),
            Predicate::Lte(t) => matches!((val.as_f64(), t.as_f64()), (Some(a), Some(b)) if a <= b),
        }
    }

    fn project(doc: &Value, select: &Option<Vec<String>>) -> Value {
        match select {
            None => doc.clone(),
            Some(fields) => {
                let mut out = serde_json::Map::new();
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

// fsync syscall
unsafe fn libc_fsync(fd: i32) {
    extern "C" { fn fsync(fd: i32) -> i32; }
    fsync(fd);
}

fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 14695981039346656037;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}