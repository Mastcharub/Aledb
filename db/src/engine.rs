use serde_json::Value;
use std::collections::HashMap;
use std::fs;
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

//   PORT                porta HTTP                      default: 3000
//   ROLE                leader | follower               default: leader
//   LEADER_URL          URL del leader                  default: http://leader:3000
//   SYNC_INTERVAL_SECS  intervallo sync follower (s)    default: 5
//   AUTOLOAD_PATH       file JSON da caricare all'avvio default: "" (nessuno)
//   SEGMENT_DIR         cartella dove salvare i segment default: ./segments
//   SEGMENT_INTERVAL_SECS  intervallo autosave (s)      default: 60
//   SHARD_KEY           campo usato per lo sharding     default: "" (nessuno)
//   SHARD_INDEX         indice di questo shard (0-based)default: 0
//   SHARD_TOTAL         numero totale di shard          default: 1

#[derive(Clone, Debug)]
pub struct Config {
    pub port:                  u16,
    pub role:                  String,
    pub leader_url:            String,
    pub sync_interval_secs:    u64,
    pub autoload_path:         Option<String>,
    pub segment_dir:           String,
    pub segment_interval_secs: u64,
    pub shard_key:             Option<String>,
    pub shard_index:           u64,
    pub shard_total:           u64,
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

        Config {
            port:                  env_u64("PORT", 3000) as u16,
            role:                  env("ROLE", "leader"),
            leader_url:            env("LEADER_URL", "http://leader:3000"),
            sync_interval_secs:    env_u64("SYNC_INTERVAL_SECS", 5),
            autoload_path,
            segment_dir:           env("SEGMENT_DIR", "./segments"),
            segment_interval_secs: env_u64("SEGMENT_INTERVAL_SECS", 60),
            shard_key,
            shard_index:           env_u64("SHARD_INDEX", 0),
            shard_total:           env_u64("SHARD_TOTAL", 1),
        }
    }
}

pub struct Aledb {
    pub config:      Config,
    data:            HashMap<String, Value>,
    index:           Index,
    modified_at:     HashMap<String, u64>,
}

impl Aledb {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            data:        HashMap::new(),
            index:       HashMap::new(),
            modified_at: HashMap::new(),
        }
    }

    pub fn autoload(&mut self) {
        if let Some(path) = self.config.autoload_path.clone() {
            match self.load(&path) {
                Ok(_)  => println!("[DB] autoloaded from {}", path),
                Err(e) => eprintln!("[DB] autoload failed ({}): {}", path, e),
            }
        }
    }

    pub fn save_segment(&self) -> Result<String, String> {
        fs::create_dir_all(&self.config.segment_dir).map_err(|e| e.to_string())?;
        let filename = format!("{}/{}.json", self.config.segment_dir, now_ms());
        self.save(&filename)?;
        Ok(filename)
    }

    // Algoritmo: hash FNV-1a del valore stringa del campo % shard_total.
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

fn fnv1a(s: &str) -> u64 {
    let mut hash: u64 = 14695981039346656037;
    for byte in s.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}