use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use uuid::Uuid;

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

}