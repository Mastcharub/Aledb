mod db;

use db::Aledb;
use serde_json::json;

fn main() {
    let mut database = Aledb::new(); 
    /*
    let doc1 = json!({"name": "Mario", "age": 32});
    let doc2 = json!({"name": "Sandro", "age": 67});
    let docs = vec![doc1, doc2];
    database.insert_many(&docs);
    database.save("dboh.json");
    */
    database.load("dboh.json");
    database.insert(json!({"name": "Alessandro", "age": 17}));
    database.save("dboh.json");
}
