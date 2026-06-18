mod engine;

use axum::{
    extract::{Path, Query as AxumQuery, State},
    routing::{get, post},
    Json, Router,
};

use engine::{aledb, Query};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    env,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time;

#[derive(Clone)]
struct AppState {
    db:   Arc<Mutex<aledb>>,
    role: Role,
}

#[derive(Clone, PartialEq)]
enum Role {
    Leader,
    Follower,
}

#[tokio::main]
async fn main() {
    let role = match env::var("ROLE").as_deref() {
        Ok("follower") => Role::Follower,
        _ => Role::Leader,
    };

    let state = AppState {
        db:   Arc::new(Mutex::new(aledb::new())),
        role: role.clone(),
    };

    if role == Role::Follower {
        let leader_url = env::var("LEADER_URL")
            .unwrap_or_else(|_| "http://leader:3000".to_string());
        let interval_secs = env::var("SYNC_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5u64);

        let db_clone = Arc::clone(&state.db);
        tokio::spawn(async move {
            sync_loop(db_clone, leader_url, interval_secs).await;
        });
    }

    let role_label = if state.role == Role::Leader { "LEADER" } else { "FOLLOWER" };

    let app = Router::new()
        .route("/insert",    post(insert))
        .route("/get/{id}",   get(get_doc))
        .route("/update/{id}", post(update_doc))
        .route("/query",     post(query))
        .route("/save",      post(save))
        .route("/load",      post(load))
        .route("/sync",      get(sync_endpoint))  // usato dai follower
        .route("/health",    get(health))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("[{}] Running on http://0.0.0.0:3000", role_label);
    axum::serve(listener, app).await.unwrap();
}

async fn sync_loop(db: Arc<Mutex<aledb>>, leader_url: String, interval_secs: u64) {
    let client  = Client::new();
    let mut last = 0u64; // timestamp dell'ultimo sync riuscito

    let mut ticker = time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await; // salta il primo tick immediato

    loop {
        ticker.tick().await;

        let url = format!("{}/sync?since={}", leader_url, last);
        let resp = match client.get(&url).send().await {
            Ok(r)  => r,
            Err(e) => { eprintln!("[FOLLOWER] sync error: {}", e); continue; }
        };

        let body: Value = match resp.json().await {
            Ok(v)  => v,
            Err(e) => { eprintln!("[FOLLOWER] parse error: {}", e); continue; }
        };

        let now_ts = body["ts"].as_u64().unwrap_or(0);
        let docs   = body["docs"].as_array().cloned().unwrap_or_default();
        let count  = docs.len();

        if !docs.is_empty() {
            let mut db = db.lock().unwrap();
            db.apply_docs(docs, now_ts);
        }

        println!("[FOLLOWER] sync ok — {} nuovi doc (since={})", count, last);
        last = now_ts;
    }
}

#[derive(Deserialize)]
struct SinceParams {
    since: Option<u64>,
}

async fn sync_endpoint(State(state): State<AppState>, AxumQuery(params): AxumQuery<SinceParams>,) -> Json<Value> {
    let since = params.since.unwrap_or(0);
    let db = state.db.lock().unwrap();
    let docs = db.docs_since(since);
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;

    Json(json!({ "ts": ts, "docs": docs }))
}

async fn insert(State(state): State<AppState>, Json(doc): Json<Value>) -> Json<Value> {
    if state.role == Role::Follower {
        return Json(json!({ "error": "follower in sola lettura — scrivi sul leader" }));
    }
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
    if state.role == Role::Follower {
        return Json(json!({ "error": "follower in sola lettura — scrivi sul leader" }));
    }
    let mut db = state.db.lock().unwrap();
    db.update(&id, patch);
    Json(json!({ "status": "ok" }))
}

async fn query(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    match Query::from_json(&payload) {
        Err(e) => Json(json!({ "error": e })),
        Ok(q)  => {
            let db      = state.db.lock().unwrap();
            let results = db.query(&q);
            Json(json!({ "count": results.len(), "results": results }))
        }
    }
}

async fn save(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"].as_str().unwrap_or("dboh.json");
    let db   = state.db.lock().unwrap();
    match db.save(path) {
        Ok(_)  => Json(json!({ "status": "saved", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn load(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"].as_str().unwrap_or("dboh.json");
    let mut db = state.db.lock().unwrap();
    match db.load(path) {
        Ok(_)  => Json(json!({ "status": "loaded", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    let role = if state.role == Role::Leader { "leader" } else { "follower" };
    Json(json!({ "status": "ok", "role": role }))
}