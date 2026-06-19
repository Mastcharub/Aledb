use criterion::{criterion_group, criterion_main, Criterion};
use serde_json::json;

use aledb::engine::{aledb, Predicate, Query};

fn build_db(size: usize) -> aledb {
    let mut db = aledb::new();

    for i in 0..size {
        db.insert(json!({
            "name": format!("user{}", i),
            "age": i % 100,
            "score": i,
            "active": i % 2 == 0
        }))
        .unwrap();
    }
    db
}

fn bench_query_eq(c: &mut Criterion) {
    let db = build_db(100_000);

    let query = Query {
        select: None,
        filters: vec![(
            "age".to_string(),
            Predicate::Eq(json!(42)),
        )],
    };

    c.bench_function("query_eq_100k", |b| {
        b.iter(|| {
            let _ = db.query(&query);
        })
    });
}

fn bench_query_range(c: &mut Criterion) {
    let db = build_db(100_000);

    let query = Query {
        select: None,
        filters: vec![
            (
                "age".to_string(),
                Predicate::Gte(json!(30)),
            ),
            (
                "age".to_string(),
                Predicate::Lte(json!(40)),
            ),
        ],
    };

    c.bench_function("query_range_100k", |b| {
        b.iter(|| {
            let _ = db.query(&query);
        })
    });
}

fn bench_query_projection(c: &mut Criterion) {
    let db = build_db(100_000);

    let query = Query {
        select: Some(vec![
            "name".to_string(),
            "age".to_string(),
        ]),
        filters: vec![(
            "active".to_string(),
            Predicate::Eq(json!(true)),
        )],
    };

    c.bench_function("query_projection_100k", |b| {
        b.iter(|| {
            let _ = db.query(&query);
        })
    });
}

fn bench_query_scaling(c: &mut Criterion) {
    let sizes = [1_000, 10_000, 100_000, 1_000_000];

    for size in sizes {
        let db = build_db(size);

        let query = Query {
            select: None,
            filters: vec![(
                "age".to_string(),
                Predicate::Eq(json!(42)),
            )],
        };

        c.bench_function(
            &format!("query_eq_{}", size),
            |b| {
                b.iter(|| {
                    let _ = db.query(&query);
                })
            },
        );
    }
}

criterion_group!(
    benches,
    bench_query_eq,
    bench_query_range,
    bench_query_projection,
    bench_query_scaling
);

criterion_main!(benches);