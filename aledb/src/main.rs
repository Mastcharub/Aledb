mod engine;

use axum::{
    extract::{Path, Query as AxumQuery, State},
    routing::{get, post, delete},
    Json, Router,
};
use engine::{Aledb, Config, FsyncMode, Query};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::{signal, time};

#[derive(Clone)]
struct AppState {
    db: Arc<RwLock<Aledb>>,
}

#[tokio::main]
async fn main() {
    let config = Config::from_env();

    let mut db = Aledb::new(config.clone());
    db.autoload();

    let state = AppState {
        db: Arc::new(RwLock::new(db)),
    };

    if config.role == "follower" {
        let db_ref        = Arc::clone(&state.db);
        let leader_url    = config.leader_url.clone();
        let interval_secs = config.sync_interval_secs;
        tokio::spawn(async move {
            sync_loop(db_ref, leader_url, interval_secs).await;
        });
    }

    {
        let db_ref = Arc::clone(&state.db);
        tokio::spawn(async move { compact_loop(db_ref).await; });
    }

    if config.fsync_mode == FsyncMode::Interval {
        let db_ref = Arc::clone(&state.db);
        let ms     = config.fsync_interval_ms;
        tokio::spawn(async move { fsync_loop(db_ref, ms).await; });
    }

    let app = Router::new()
        .route("/insert",             post(insert))
        .route("/get/{id}",           get(get_doc))
        .route("/update/{id}",        post(update_doc))
        .route("/delete/{id}",        delete(delete_doc))
        .route("/query",              post(query))
        .route("/save",               post(save))
        .route("/load",               post(load))
        .route("/sync",               get(sync_endpoint))
        .route("/migrate/export",     post(migrate_export))
        .route("/migrate/import",     post(migrate_import))
        .route("/collections",        get(list_collections))
        .route("/health",             get(health))
        .with_state(state.clone());

    let addr = format!("0.0.0.0:{}", config.port);
    println!("[{}] listening on {}", config.role.to_uppercase(), addr);

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state.db))
        .await
        .unwrap();
}

async fn shutdown_signal(db: Arc<RwLock<Aledb>>) {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to install Ctrl+C handler");
    };
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! {
        _ = ctrl_c    => {},
        _ = terminate => {},
    }
    println!("[shutdown] signal received, flushing WAL...");
    let mut db = db.write().unwrap();
    db.flush_fsync();
    if let Err(e) = db.compact() {
        eprintln!("[shutdown] compact error: {}", e);
    }
    println!("[shutdown] done");
}

async fn compact_loop(db: Arc<RwLock<Aledb>>) {
    let mut ticker = time::interval(Duration::from_secs(30));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        if let Err(e) = db.write().unwrap().compact() {
            eprintln!("[COMPACT] error: {}", e);
        }
    }
}

async fn fsync_loop(db: Arc<RwLock<Aledb>>, interval_ms: u64) {
    let mut ticker = time::interval(Duration::from_millis(interval_ms));
    ticker.tick().await;
    loop {
        ticker.tick().await;
        db.write().unwrap().flush_fsync();
    }
}

async fn sync_loop(db: Arc<RwLock<Aledb>>, leader_url: String, interval_secs: u64) {
    let client      = Client::new();
    let mut last    = 0u64;
    let mut backoff = interval_secs;
    let max_backoff = interval_secs * 32;

    loop {
        time::sleep(Duration::from_secs(backoff)).await;

        let url  = format!("{}/sync?since={}", leader_url, last);
        let resp = match client.get(&url).send().await {
            Ok(r)  => { backoff = interval_secs; r }
            Err(e) => {
                eprintln!("[FOLLOWER] sync error: {} (retry in {}s)", e, backoff);
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
        };
        let body: Value = match resp.json().await {
            Ok(v)  => v,
            Err(e) => { eprintln!("[FOLLOWER] parse error: {}", e); continue; }
        };

        let now_ts = body["ts"].as_u64().unwrap_or(0);
        let docs   = body["docs"].as_array().cloned().unwrap_or_default();
        let count  = docs.len();

        if !docs.is_empty() {
            db.write().unwrap().apply_docs(docs, now_ts);
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
    let db    = state.db.read().unwrap();
    let docs  = db.docs_since(since);
    let ts    = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    Json(json!({ "ts": ts, "docs": docs }))
}

#[derive(Deserialize)]
struct ExportParams {
    shard_key: String,
    tenant_id: String,
    #[serde(default)]
    delete: bool,
}

async fn migrate_export(
    State(state): State<AppState>,
    Json(params): Json<Value>,
) -> Json<Value> {
    let shard_key = match params["shard_key"].as_str() {
        Some(k) => k.to_string(),
        None    => return Json(json!({ "error": "shard_key mancante" })),
    };
    let tenant_id = match params["tenant_id"].as_str() {
        Some(t) => t.to_string(),
        None    => return Json(json!({ "error": "tenant_id mancante" })),
    };
    let do_delete = params["delete"].as_bool().unwrap_or(false);

    let docs = state.db.read().unwrap().docs_for_tenant(&shard_key, &tenant_id);

    if do_delete {
        state.db.write().unwrap().delete_tenant(&shard_key, &tenant_id);
    }

    Json(json!({ "count": docs.len(), "docs": docs }))
}

async fn migrate_import(
    State(state): State<AppState>,
    Json(payload): Json<Value>,
) -> Json<Value> {
    let docs = match payload["docs"].as_array() {
        Some(d) => d.clone(),
        None    => return Json(json!({ "error": "docs mancante" })),
    };
    let count = docs.len();
    let ts    = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    state.db.write().unwrap().apply_docs(docs, ts);
    Json(json!({ "imported": count }))
}

async fn insert(State(state): State<AppState>, Json(mut payload): Json<Value>) -> Json<Value> {
    let mut db = state.db.write().unwrap();
    if db.config.role == "follower" {
        return Json(json!({ "error": "follower in sola lettura" }));
    }
    let collection = payload.get("_collection")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    if let Some(obj) = payload.as_object_mut() {
        obj.remove("_collection");
    }
    let coll_ref = collection.as_deref();
    match db.insert(payload, coll_ref) {
        Ok(id) => Json(json!({ "id": id, "collection": collection })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn get_doc(State(state): State<AppState>, Path(id): Path<String>) -> Json<Value> {
    let db = state.db.read().unwrap();
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
    let mut db = state.db.write().unwrap();
    if db.config.role == "follower" {
        return Json(json!({ "error": "follower in sola lettura" }));
    }
    db.update(&id, patch);
    Json(json!({ "status": "ok" }))
}

async fn delete_doc(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Json<Value> {
    let mut db = state.db.write().unwrap();
    if db.config.role == "follower" {
        return Json(json!({ "error": "follower in sola lettura" }));
    }
    db.delete(&id);
    Json(json!({ "status": "ok" }))
}

async fn query(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    match Query::from_json(&payload) {
        Err(e) => Json(json!({ "error": e })),
        Ok(q)  => {
            let db      = state.db.read().unwrap();
            let results = db.query(&q);
            Json(json!({ "count": results.len(), "results": results }))
        }
    }
}

async fn save(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"].as_str().unwrap_or("dboh.json").to_string();
    let db   = state.db.read().unwrap();
    match db.save(&path) {
        Ok(_)  => Json(json!({ "status": "saved", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn load(State(state): State<AppState>, Json(payload): Json<Value>) -> Json<Value> {
    let path = payload["path"].as_str().unwrap_or("dboh.json").to_string();
    let mut db = state.db.write().unwrap();
    match db.load(&path) {
        Ok(_)  => Json(json!({ "status": "loaded", "file": path })),
        Err(e) => Json(json!({ "error": e })),
    }
}

async fn list_collections(State(state): State<AppState>) -> Json<Value> {
    let db   = state.db.read().unwrap();
    let list = db.list_collections();
    let detail: Vec<Value> = list.iter().map(|(name, count)| {
        json!({ "name": name, "count": count })
    }).collect();
    Json(json!({ "collections": detail }))
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    let db = state.db.read().unwrap();
    Json(json!({
        "status":      "ok",
        "role":        db.config.role,
        "shard_index": db.config.shard_index,
        "shard_total": db.config.shard_total,
        "fsync":       format!("{:?}", db.config.fsync_mode),
    }))
}