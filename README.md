# sea-orm-timescale

TimescaleDB extension for Sea-ORM.

## Install

```bash
cargo add sea-orm-timescale
```

## Query Functions

### time_bucket

```rust
use sea_orm_timescale::{functions::time_bucket, types::Interval};

let bucket = time_bucket(&Interval::Hours(1), readings::Column::Time);
// SQL: time_bucket('1 hours', "time")
```

### time_bucket_gapfill

```rust
use sea_orm_timescale::functions::time_bucket_gapfill;

let bucket = time_bucket_gapfill(&Interval::Hours(1), readings::Column::Time);
// SQL: time_bucket_gapfill('1 hours', "time")
```

### first / last

```rust
use sea_orm_timescale::functions::{first, last};

let earliest = first(readings::Column::Value, readings::Column::Time);
let latest = last(readings::Column::Value, readings::Column::Time);
```

### locf (Last Observation Carried Forward)

```rust
use sea_orm_timescale::functions::locf;
use sea_orm::entity::prelude::*;

let filled = locf(Expr::col(readings::Column::Value).avg());
// SQL: locf(AVG("value"))
```

### histogram

```rust
use sea_orm_timescale::functions::histogram;

let dist = histogram(readings::Column::Temperature, 0.0, 100.0, 10);
// SQL: histogram("temperature", 0, 100, 10)
```

## Migration Helpers

### Create a hypertable

```rust
use sea_orm_timescale::{migration::create_hypertable, types::{HypertableConfig, Interval}};

create_hypertable(&db, &HypertableConfig {
    table_name: "readings".into(),
    time_column: "time".into(),
    chunk_interval: Some(Interval::Days(7)),
    if_not_exists: true,
}).await?;
```

### Enable compression

```rust
use sea_orm_timescale::{migration::enable_compression, types::*};

enable_compression(&db, "readings", &CompressionConfig {
    segment_by: vec!["site_id".into()],
    order_by: vec![("time".into(), SortDirection::Desc)],
    compress_after: Interval::Days(30),
}).await?;
```

### Add retention policy

```rust
use sea_orm_timescale::{migration::add_retention_policy, types::RetentionConfig};

add_retention_policy(&db, "readings", &RetentionConfig {
    drop_after: Interval::Days(365),
}).await?;
```

### Create continuous aggregate

```rust
use sea_orm_timescale::{migration::create_continuous_aggregate, types::*};

create_continuous_aggregate(&db,
    "SELECT time_bucket('1 hour', time) AS bucket,
            site_id, AVG(value) AS avg_value
     FROM readings GROUP BY bucket, site_id",
    &ContinuousAggregateConfig {
        view_name: "readings_hourly".into(),
        bucket_interval: Interval::Hours(1),
        refresh_policy: Some(RefreshPolicy {
            start_offset: Interval::Days(3),
            end_offset: Interval::Hours(1),
            schedule_interval: Interval::Hours(1),
        }),
    },
).await?;
```

### Refresh continuous aggregate

```rust
use sea_orm_timescale::migration::refresh_continuous_aggregate;

refresh_continuous_aggregate(&db, "readings_hourly", "2024-01-01", "2024-02-01").await?;
```

## Interval Parsing

Supports full and short formats:

```rust
use sea_orm_timescale::types::Interval;

// Full
Interval::parse("1 hour");    // Interval::Hours(1)
Interval::parse("5 minutes"); // Interval::Minutes(5)
Interval::parse("7 days");    // Interval::Days(7)

// Short
Interval::parse("1h");  // Interval::Hours(1)
Interval::parse("5m");  // Interval::Minutes(5)
Interval::parse("7d");  // Interval::Days(7)
Interval::parse("1w");  // Interval::Weeks(1)
Interval::parse("30s"); // Interval::Seconds(30)
Interval::parse("1M");  // Interval::Months(1)
```

## Security

All identifier parameters are validated to prevent SQL injection:

- `validate_ident()` — rejects non-alphanumeric/underscore identifiers
- `escape_string_literal()` — escapes single quotes in string values

## License

MIT
