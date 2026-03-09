//! End-to-end tests against a real TimescaleDB instance.
//!
//! These tests are `#[ignore]` by default and require:
//! - A running TimescaleDB instance with the `timescaledb` extension enabled
//! - `DATABASE_URL` environment variable set
//!
//! Run with: `cargo test -- --ignored --test-threads=1`

use sea_orm::sea_query::{Alias, SimpleExpr};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, DbErr, Statement};
use sea_orm_timescale::functions::*;
use sea_orm_timescale::migration::*;
use sea_orm_timescale::types::*;
use uuid::Uuid;

async fn connect_db() -> DatabaseConnection {
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for E2E tests");
    let db = Database::connect(&url)
        .await
        .expect("Failed to connect to database");

    // Ensure TimescaleDB extension is enabled
    db.execute_unprepared("CREATE EXTENSION IF NOT EXISTS timescaledb")
        .await
        .expect("Failed to create timescaledb extension");

    db
}

fn unique_name(prefix: &str) -> String {
    let id = Uuid::new_v4().simple().to_string();
    format!("{}_{}", prefix, &id[..8])
}

fn expr_sql(expr: &SimpleExpr) -> &str {
    match expr {
        SimpleExpr::Custom(s) => s.as_str(),
        _ => panic!("Expected SimpleExpr::Custom"),
    }
}

async fn create_test_table(db: &DatabaseConnection, name: &str) {
    let sql = format!(
        r#"CREATE TABLE "{name}" (
            time TIMESTAMPTZ NOT NULL,
            sensor_id TEXT NOT NULL,
            value DOUBLE PRECISION NOT NULL
        )"#
    );
    db.execute_unprepared(&sql)
        .await
        .expect("Failed to create test table");
}

async fn drop_table(db: &DatabaseConnection, name: &str) {
    let _ = db
        .execute_unprepared(&format!(r#"DROP TABLE IF EXISTS "{name}" CASCADE"#))
        .await;
}

async fn drop_view(db: &DatabaseConnection, name: &str) {
    let _ = db
        .execute_unprepared(&format!(
            r#"DROP MATERIALIZED VIEW IF EXISTS "{name}" CASCADE"#
        ))
        .await;
}

/// Creates a hypertable with sample data spanning 3 hours across 2 sensors.
async fn setup_hypertable_with_data(db: &DatabaseConnection) -> String {
    let table = unique_name("ts");
    create_test_table(db, &table).await;

    create_hypertable(
        db,
        &HypertableConfig {
            table_name: table.clone(),
            time_column: "time".into(),
            chunk_interval: Some(Interval::Days(7)),
            if_not_exists: true,
        },
    )
    .await
    .unwrap();

    let sql = format!(
        r#"INSERT INTO "{table}" (time, sensor_id, value) VALUES
            ('2024-01-01 00:10:00+00', 'a', 10.0),
            ('2024-01-01 00:40:00+00', 'a', 20.0),
            ('2024-01-01 01:10:00+00', 'a', 30.0),
            ('2024-01-01 01:40:00+00', 'b', 40.0),
            ('2024-01-01 02:10:00+00', 'b', 50.0)"#
    );
    db.execute_unprepared(&sql).await.unwrap();

    table
}

#[tokio::test]
#[ignore]
async fn test_create_hypertable() {
    let db = connect_db().await;
    let table = unique_name("ht");
    create_test_table(&db, &table).await;

    create_hypertable(
        &db,
        &HypertableConfig {
            table_name: table.clone(),
            time_column: "time".into(),
            chunk_interval: Some(Interval::Days(7)),
            if_not_exists: true,
        },
    )
    .await
    .expect("create_hypertable should succeed");

    // Verify it appears in the TimescaleDB catalog
    let row = db
        .query_one(Statement::from_string(
            DbBackend::Postgres,
            format!(
                "SELECT hypertable_name FROM timescaledb_information.hypertables \
                 WHERE hypertable_name = '{table}'"
            ),
        ))
        .await
        .expect("catalog query failed");
    assert!(row.is_some(), "Table should appear in hypertables catalog");

    drop_table(&db, &table).await;
}

#[tokio::test]
#[ignore]
async fn test_time_bucket_query() {
    let db = connect_db().await;
    let table = setup_hypertable_with_data(&db).await;

    let bucket = time_bucket(&Interval::Hours(1), Alias::new("time"));
    let sql = format!(
        r#"SELECT {bucket} AS bucket, AVG(value) AS avg_val
           FROM "{table}" GROUP BY bucket ORDER BY bucket"#,
        bucket = expr_sql(&bucket)
    );
    let rows = db
        .query_all(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .expect("time_bucket query failed");

    // Data spans hours 00, 01, 02 -> 3 buckets
    assert_eq!(rows.len(), 3, "Expected 3 hourly buckets");

    drop_table(&db, &table).await;
}

#[tokio::test]
#[ignore]
async fn test_first_last_aggregates() {
    let db = connect_db().await;
    let table = setup_hypertable_with_data(&db).await;

    let first_expr = first(Alias::new("value"), Alias::new("time"));
    let last_expr = last(Alias::new("value"), Alias::new("time"));
    let sql = format!(
        r#"SELECT {first} AS first_val, {last} AS last_val FROM "{table}""#,
        first = expr_sql(&first_expr),
        last = expr_sql(&last_expr)
    );

    let row = db
        .query_one(Statement::from_string(DbBackend::Postgres, sql))
        .await
        .expect("first/last query failed")
        .expect("Should return a row");

    let first_val: f64 = row.try_get("", "first_val").unwrap();
    let last_val: f64 = row.try_get("", "last_val").unwrap();

    assert_eq!(first_val, 10.0, "first() should return earliest value");
    assert_eq!(last_val, 50.0, "last() should return latest value");

    drop_table(&db, &table).await;
}

#[tokio::test]
#[ignore]
async fn test_enable_compression() {
    let db = connect_db().await;
    let table = setup_hypertable_with_data(&db).await;

    enable_compression(
        &db,
        &table,
        &CompressionConfig {
            segment_by: vec!["sensor_id".into()],
            order_by: vec![("time".into(), SortDirection::Desc)],
            compress_after: Interval::Days(30),
        },
    )
    .await
    .expect("enable_compression should succeed");

    drop_table(&db, &table).await;
}

#[tokio::test]
#[ignore]
async fn test_add_retention_policy() {
    let db = connect_db().await;
    let table = setup_hypertable_with_data(&db).await;

    add_retention_policy(
        &db,
        &table,
        &RetentionConfig {
            drop_after: Interval::Days(365),
        },
    )
    .await
    .expect("add_retention_policy should succeed");

    drop_table(&db, &table).await;
}

#[tokio::test]
#[ignore]
async fn test_continuous_aggregate() {
    let db = connect_db().await;
    let table = setup_hypertable_with_data(&db).await;
    let view = unique_name("cagg");

    let select_sql = format!(
        r#"SELECT time_bucket('1 hour', time) AS bucket,
                  sensor_id,
                  AVG(value) AS avg_value
           FROM "{table}"
           GROUP BY bucket, sensor_id"#
    );

    create_continuous_aggregate(
        &db,
        &select_sql,
        &ContinuousAggregateConfig {
            view_name: view.clone(),
            bucket_interval: Interval::Hours(1),
            refresh_policy: None,
        },
    )
    .await
    .expect("create_continuous_aggregate should succeed");

    refresh_continuous_aggregate(&db, &view, "2024-01-01", "2024-01-02")
        .await
        .expect("refresh_continuous_aggregate should succeed");

    // Verify data materialized in the view
    let rows = db
        .query_all(Statement::from_string(
            DbBackend::Postgres,
            format!(r#"SELECT * FROM "{view}""#),
        ))
        .await
        .expect("query on continuous aggregate failed");

    assert!(
        !rows.is_empty(),
        "Continuous aggregate should contain data after refresh"
    );

    drop_view(&db, &view).await;
    drop_table(&db, &table).await;
}

#[tokio::test]
#[ignore]
async fn test_invalid_identifiers() {
    let db = connect_db().await;

    // create_hypertable rejects SQL injection in table name
    let result = create_hypertable(
        &db,
        &HypertableConfig {
            table_name: "table; DROP TABLE users".into(),
            time_column: "time".into(),
            chunk_interval: None,
            if_not_exists: false,
        },
    )
    .await;

    assert!(result.is_err(), "Should reject invalid table name");
    match result.unwrap_err() {
        DbErr::Custom(msg) => {
            assert!(
                msg.contains("Invalid SQL identifier"),
                "Unexpected error: {msg}"
            );
        }
        other => panic!("Expected DbErr::Custom, got: {other:?}"),
    }

    // enable_compression rejects bad column names
    let table = setup_hypertable_with_data(&db).await;
    let result = enable_compression(
        &db,
        &table,
        &CompressionConfig {
            segment_by: vec!["bad column!".into()],
            order_by: vec![],
            compress_after: Interval::Days(30),
        },
    )
    .await;

    assert!(result.is_err(), "Should reject invalid column name");
    match result.unwrap_err() {
        DbErr::Custom(msg) => {
            assert!(
                msg.contains("Invalid SQL identifier"),
                "Unexpected error: {msg}"
            );
        }
        other => panic!("Expected DbErr::Custom, got: {other:?}"),
    }

    drop_table(&db, &table).await;
}
