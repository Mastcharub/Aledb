mod engine;

use axum::{
    extract::{Path, Query as AxumQuery, State},
    routing::{get, post},
    Json, Router,
};

use engine::{Aledb, Config, Query};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time;

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Aledb>>,
}

#[tokio::main]
async fn main() {
    let config = Config::from_env();

    let mut db = Aledb::new(config.clone());
    db.autoload();

    let state = AppState {
        db: Arc::new(Mutex::new(db)),
    };

    if config.role == "follower" {
        let db_ref          = Arc::clone(&state.db);
        let leader_url      = config.leader_url.clone();
        let interval_secs   = config.sync_interval_secs;
        tokio::spawn(async move {
            sync_loop(db_ref, leader_url, interval_secs).await;
        });
    }

    let app = Router::new()
        .route("/insert",      post(insert))
        .route("/get/{id}",    get(get_doc))
        .route("/update/{id}", post(update_doc))
        .route("/query",       post(query))
        .route("/save",        post(save))
        .route("/load",        post(load))
        .route("/sync",        get(sync_endpoint))
        .route("/health",      get(health))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", config.port);
    println!("[{}] listening on {}", config.role.to_uppercase(), addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn sync_loop(db: Arc<Mutex<Aledb>>, leader_url: String, interval_secs: u64) {
    let client   = Client::new();
    let mut last = 0u64;

    let mut ticker = time::interval(Duration::from_secs(interval_secs));
    ticker.tick().await;

    loop {
        ticker.tick().await;

        let url  = format!("{}/sync?since={}", leader_url, last);
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
            db.lock().unwrap().apply_docs(docs, now_ts);
        }

        println!("[FOLLOWER] sync ok — {} doc (since={})", count, last);
        last = now_ts;
    }
}

#[derive(Deserialize)]
struct SinceParams { since: Option<u64> }

async fn sync_endpoint(
    State(state): State<AppState>,
    AxumQuery(params): AxumQuery<SinceParams>,
) -> Json<Value> {
    let since = params.since.unwrap_or(0);
    let db    = state.db.lock().unwrap();
    let docs  = db.docs_since(since);
    let ts    = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    Json(json!({ "ts": ts, "docs": docs }))
}

async fn insert(State(state): State<AppState>, Json(doc): Json<Value>) -> Json<Value> {
    let mut db = state.db.lock().unwrap();
    if db.config.role == "follower" {
        return Json(json!({ "error": "follower in sola lettura" }));
    }
    match db.insert(doc) {
        Ok(id) => Json(json!({ "id": id })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn get_doc(State(state): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let db = state.db.lock().unwrap();
    match db.get_id(&id) {
        Some(doc) => Json(doc),
        None      => Json(json!({ "error": "not found" })),
    }
}

async fn update_doc(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(patch): Json<Value>,
) -> Json<Value> {
    let mut db = state.db.lock().unwrap();
    if db.config.role == "follower" {
        return Json(json!({ "error": "follower in sola lettura" }));
    }
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
    let path = payload["path"].as_str().unwrap_or("dboh.json").to_string();
    let db   = state.db.lock().unwrap();
    match db.save(&path) {
        Ok(_)  => Json(json!({ "status": "saved", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn load(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"].as_str().unwrap_or("dboh.json").to_string();
    let mut db = state.db.lock().unwrap();
    match db.load(&path) {
        Ok(_)  => Json(json!({ "status": "loaded", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    let db = state.db.lock().unwrap();
    Json(json!({
        "status":       "ok",
        "role":         db.config.role,
        "shard_index":  db.config.shard_index,
        "shard_total":  db.config.shard_total,
    }))
}