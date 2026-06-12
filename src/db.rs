use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use uuid::Uuid;

pub struct Aledb {
    data: HashMap<String, Value>,
}

impl Aledb {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /*
    pub fn insert(&mut self, doc: Value) {
        let id = doc["id"].as_str().unwrap().to_string();
        self.data.insert(id, doc);
    }
    */

    pub fn insert(&mut self, mut doc: Value) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        
        if !doc.is_object() {
            return Err("Il documento deve essere un oggetto JSON".to_string());
        }

        doc["id"] = Value::String(id.clone());

        if self.data.contains_key(&id) {
            return Err("ID collisione (impossibile con UUID)".to_string());
        }

        self.data.insert(id.clone(), doc);
        Ok(id)
    }

    pub fn insert_many(&mut self, docs: &[Value]) {
        for doc in docs {
            self.insert(doc.clone());
        }
    }

    /// trova per id
    pub fn get_id(&self, id: &str) -> Option<&Value> {
        self.data.get(id)
    }
    
    /*
    pub fn save(&self, path: &str) {
        let vec: Vec<&Value> = self.data.values().collect();
        let json = serde_json::to_string_pretty(&vec).unwrap();
        fs::write(path, json).unwrap();
    }
    */    

    pub fn save(&self, path: &str) -> Result<(), String> {
        let docs: Vec<&Value> = self.data.values().collect();

        let json = serde_json::to_string_pretty(&docs).map_err(|e| e.to_string())?;

        fs::write(path, json).map_err(|e| e.to_string())?;

        Ok(())
    }

    pub fn load(&mut self, path: &str) {
        let content = fs::read_to_string(path).unwrap_or("[]".to_string());
        let vec: Vec<Value> = serde_json::from_str(&content).unwrap_or(vec![]);

        for doc in vec {
            self.insert(doc);
        }
    }
    pub fn update(&mut self, id: &str, patch: Value) {
        if let Some(existing) = self.data.get_mut(id) {
            if let (Value::Object(existing_map), Value::Object(patch_map)) = (existing, patch){
                for (k, v) in patch_map {
                    existing_map.insert(k, v);
                }
            }
        }
    }
}
                                    
