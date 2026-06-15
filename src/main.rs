mod engine;

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use engine::Aledb;
use engine::Query;

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Aledb>>,
}

#[tokio::main]
async fn main() {
    let state = AppState {
        db: Arc::new(Mutex::new(Aledb::new())),
    };

    let app = Router::new()
        .route("/insert", post(insert))
        .route("/get/:id", get(get_doc))
        .route("/update/:id", post(update_doc))
        .route("/save", post(save))
        .route("/load", post(load))
        .route("/query", post(query)) 
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    println!("Server running on http://localhost:3000");

    axum::serve(listener, app).await.unwrap();
}

async fn insert(State(state): State<AppState>, Json(doc): Json<Value>) -> Json<Value> {
    let mut db = state.db.lock().unwrap();

    match db.insert(doc) {
        Ok(id) => Json(json!({ "id": id })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn get_doc(State(state): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let db = state.db.lock().unwrap();

    match db.get_id(&id) {
        Some(doc) => Json(doc),
        None => Json(json!({ "error": "not found" })),
    }
}

async fn update_doc(State(state): State<AppState>, Path(id): Path<String>, Json(patch): Json<Value>) -> Json<Value> {
    let mut db = state.db.lock().unwrap();
    db.update(&id, patch);

    Json(json!({ "status": "ok" }))
}

async fn save(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"]
        .as_str()
        .unwrap_or("dboh.json");
    let db = state.db.lock().unwrap();

    match db.save(path) {
        Ok(_) => Json(json!({
            "status": "saved",
            "file": path
        })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn load(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"].as_str().unwrap_or("dboh.json");
    let mut db = state.db.lock().unwrap();

    match db.load(path) {
        Ok(_) => Json(json!({ "status": "loaded", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

//   {
//     "select": ["nome", "età"],
//     "where": {
//       "città":  "Milano",               
//       "età":    { ">": 25 },
//       "score":  { ">=": 9.5 }
//      }
//   }

async fn query(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    match Query::from_json(&payload) {
        Err(e) => Json(json!({ "error": e })),
        Ok(q)  => {
            let db = state.db.lock().unwrap();
            let results = db.query(&q);
            Json(json!({
                "count":   results.len(),
                "results": results,
            }))
        }
    }
}