use sea_query::{IntoIden, PostgresQueryBuilder, Query, SimpleExpr};

use crate::types::Interval;

/// Generates a `time_bucket(interval, column)` expression for use in Sea-ORM queries.
///
/// # Example
/// ```ignore
/// use sea_orm_timescale::{functions::time_bucket, types::Interval};
///
/// let bucket = time_bucket(&Interval::Hours(1), readings::Column::Time);
/// // SQL: time_bucket('1 hours', "time")
/// ```
pub fn time_bucket(interval: &Interval, column: impl IntoIden + Clone) -> SimpleExpr {
    let iden = column.into_iden();
    let col_name = iden.to_string();
    SimpleExpr::Custom(format!("time_bucket('{interval}', \"{col_name}\")"))
}

/// Generates a `time_bucket_gapfill(interval, column)` expression.
///
/// Used with `locf()` or `interpolate()` to fill gaps in time-series data.
pub fn time_bucket_gapfill(interval: &Interval, column: impl IntoIden + Clone) -> SimpleExpr {
    let iden = column.into_iden();
    let col_name = iden.to_string();
    SimpleExpr::Custom(format!("time_bucket_gapfill('{interval}', \"{col_name}\")"))
}

/// Generates a `first(value_column, time_column)` aggregate expression.
///
/// Returns the value of `value_column` at the earliest `time_column` in the group.
pub fn first(value_col: impl IntoIden + Clone, time_col: impl IntoIden + Clone) -> SimpleExpr {
    let value_name = value_col.into_iden().to_string();
    let time_name = time_col.into_iden().to_string();
    SimpleExpr::Custom(format!("first(\"{value_name}\", \"{time_name}\")"))
}

/// Generates a `last(value_column, time_column)` aggregate expression.
///
/// Returns the value of `value_column` at the latest `time_column` in the group.
pub fn last(value_col: impl IntoIden + Clone, time_col: impl IntoIden + Clone) -> SimpleExpr {
    let value_name = value_col.into_iden().to_string();
    let time_name = time_col.into_iden().to_string();
    SimpleExpr::Custom(format!("last(\"{value_name}\", \"{time_name}\")"))
}

/// Wraps a `SimpleExpr` with `locf()` (Last Observation Carried Forward).
///
/// Used with `time_bucket_gapfill` to fill NULL gaps with the last known value.
///
/// # Example
/// ```ignore
/// use sea_orm::entity::prelude::*;
/// use sea_orm_timescale::functions::{time_bucket_gapfill, locf};
/// use sea_orm_timescale::types::Interval;
///
/// let avg = Expr::col(readings::Column::Value).avg();
/// let filled = locf(avg);
/// // SQL: locf(AVG("value"))
/// ```
pub fn locf(inner: SimpleExpr) -> SimpleExpr {
    match inner {
        SimpleExpr::Custom(sql) => SimpleExpr::Custom(format!("locf({sql})")),
        other => {
            // For non-Custom exprs, render via sea-query and wrap
            let rendered = Query::select().expr(other).to_string(PostgresQueryBuilder);
            // Extract just the expression from "SELECT <expr>"
            let expr_str = rendered.strip_prefix("SELECT ").unwrap_or(&rendered);
            SimpleExpr::Custom(format!("locf({expr_str})"))
        }
    }
}

/// Generates a `histogram(column, min, max, num_buckets)` expression.
///
/// Returns an array of counts representing the distribution of values.
pub fn histogram(column: impl IntoIden + Clone, min: f64, max: f64, buckets: i32) -> SimpleExpr {
    let col_name = column.into_iden().to_string();
    SimpleExpr::Custom(format!(
        "histogram(\"{col_name}\", {min}, {max}, {buckets})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::Alias;

    /// Extract the SQL string from a SimpleExpr::Custom variant.
    fn custom_sql(expr: &SimpleExpr) -> &str {
        match expr {
            SimpleExpr::Custom(s) => s.as_str(),
            _ => panic!("Expected SimpleExpr::Custom"),
        }
    }

    #[test]
    fn test_time_bucket_sql() {
        let expr = time_bucket(&Interval::Hours(1), Alias::new("time"));
        assert_eq!(custom_sql(&expr), "time_bucket('1 hours', \"time\")");
    }

    #[test]
    fn test_time_bucket_gapfill_sql() {
        let expr = time_bucket_gapfill(&Interval::Minutes(30), Alias::new("timestamp"));
        assert_eq!(
            custom_sql(&expr),
            "time_bucket_gapfill('30 minutes', \"timestamp\")"
        );
    }

    #[test]
    fn test_first_sql() {
        let expr = first(Alias::new("value"), Alias::new("time"));
        assert_eq!(custom_sql(&expr), "first(\"value\", \"time\")");
    }

    #[test]
    fn test_last_sql() {
        let expr = last(Alias::new("value"), Alias::new("time"));
        assert_eq!(custom_sql(&expr), "last(\"value\", \"time\")");
    }

    #[test]
    fn test_locf_with_custom_expr() {
        let inner = SimpleExpr::Custom("AVG(\"value\")".to_string());
        let expr = locf(inner);
        assert_eq!(custom_sql(&expr), "locf(AVG(\"value\"))");
    }

    #[test]
    fn test_histogram_sql() {
        let expr = histogram(Alias::new("temperature"), 0.0, 100.0, 10);
        assert_eq!(custom_sql(&expr), "histogram(\"temperature\", 0, 100, 10)");
    }

    #[test]
    fn test_time_bucket_various_intervals() {
        let cases = vec![
            (Interval::Seconds(30), "time_bucket('30 seconds', \"ts\")"),
            (Interval::Minutes(5), "time_bucket('5 minutes', \"ts\")"),
            (Interval::Days(1), "time_bucket('1 days', \"ts\")"),
            (Interval::Weeks(1), "time_bucket('1 weeks', \"ts\")"),
            (Interval::Months(1), "time_bucket('1 months', \"ts\")"),
        ];

        for (interval, expected) in cases {
            let expr = time_bucket(&interval, Alias::new("ts"));
            assert_eq!(custom_sql(&expr), expected);
        }
    }
}
