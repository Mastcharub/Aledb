use serde_json::Value;
use std::collections::HashMap;
use std::fs;
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
        // --- SELECT ---
        let select = match raw.get("select") {
            Some(Value::Array(arr)) => {
                let fields: Result<Vec<String>, _> = arr.iter().map(|v| {
                        v.as_str().map(|s| s.to_string()).ok_or("I campi in 'select' devono essere stringhe")
                    }).collect();
                Some(fields?)
            }
            Some(_) => return Err("'select' deve essere un array di stringhe".to_string()),
            None => None,
        };
 
        // --- WHERE ---
        let mut filters = Vec::new();
 
        if let Some(Value::Object(where_map)) = raw.get("where") {
            for (field, condition) in where_map {
                let predicate = if condition.is_object() {
                    let obj = condition.as_object().unwrap();
                    if obj.len() != 1 {
                        return Err(format!(
                            "Il campo '{}' deve avere esattamente un operatore", field
                        ));
                    }
                    let (op, val) = obj.iter().next().unwrap();
                    match op.as_str() {
                        "="  | "eq"  => Predicate::Eq(val.clone()),
                        ">"  | "gt"  => Predicate::Gt(val.clone()),
                        ">=" | "gte" => Predicate::Gte(val.clone()),
                        "<"  | "lt"  => Predicate::Lt(val.clone()),
                        "<=" | "lte" => Predicate::Lte(val.clone()),
                        other => {
                            return Err(format!("Operatore non supportato: '{}'", other))
                        }
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

#[derive(Default)]
pub struct Aledb {
    data: HashMap<String, Value>,
}

impl Aledb {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, mut doc: Value) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();

        if !doc.is_object() {
            return Err("Il documento deve essere un oggetto JSON".to_string());
        }

        doc["id"] = Value::String(id.clone());
        self.data.insert(id.clone(), doc);

        Ok(id)
    }

    pub fn get_id(&self, id: &str) -> Option<Value> {
        self.data.get(id).cloned()
    }

    pub fn update(&mut self, id: &str, patch: Value) {
        if let Some(existing) = self.data.get_mut(id) {
            if let (Value::Object(e), Value::Object(p)) = (existing, patch) {
                for (k, v) in p {
                    e.insert(k, v);
                }
            }
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
            self.data.insert(id, doc);
        }

        Ok(())
    }

    
    pub fn query(&self, q: &Query) -> Vec<Value> {
        self.data
            .values()
            .filter(|doc| Self::matches(doc, &q.filters))
            .map(|doc| Self::project(doc, &q.select))
            .collect()
    }

    /// Restituisce true se il documento soddisfa tutti i predicati.
    fn matches(doc: &Value, filters: &[(String, Predicate)]) -> bool {
        filters.iter().all(|(field, pred)| {
            let Some(field_val) = doc.get(field) else {
                return false;
            };
            Self::eval(field_val, pred)
        })
    }
 
    fn eval(val: &Value, pred: &Predicate) -> bool {
        match pred {
            Predicate::Eq(expected) => val == expected,
 
            Predicate::Gt(threshold) => {
                matches!((val.as_f64(), threshold.as_f64()), (Some(a), Some(b)) if a > b)
            }
            Predicate::Gte(threshold) => {
                matches!((val.as_f64(), threshold.as_f64()), (Some(a), Some(b)) if a >= b)
            }
            Predicate::Lt(threshold) => {
                matches!((val.as_f64(), threshold.as_f64()), (Some(a), Some(b)) if a < b)
            }
            Predicate::Lte(threshold) => {
                matches!((val.as_f64(), threshold.as_f64()), (Some(a), Some(b)) if a <= b)
            }
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