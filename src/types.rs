use std::fmt;

/// Represents a PostgreSQL/TimescaleDB interval for use in time_bucket, retention policies, etc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Interval {
    Microseconds(i64),
    Milliseconds(i64),
    Seconds(i64),
    Minutes(i64),
    Hours(i64),
    Days(i64),
    Weeks(i64),
    Months(i64),
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Microseconds(n) => write!(f, "{n} microseconds"),
            Self::Milliseconds(n) => write!(f, "{n} milliseconds"),
            Self::Seconds(n) => write!(f, "{n} seconds"),
            Self::Minutes(n) => write!(f, "{n} minutes"),
            Self::Hours(n) => write!(f, "{n} hours"),
            Self::Days(n) => write!(f, "{n} days"),
            Self::Weeks(n) => write!(f, "{n} weeks"),
            Self::Months(n) => write!(f, "{n} months"),
        }
    }
}

impl Interval {
    /// Parse a human-readable interval string like "1 hour", "5 minutes", "1h", "30m".
    ///
    /// Supported formats:
    /// - Full: "1 hour", "5 minutes", "7 days"
    /// - Short: "1h", "5m", "7d", "1w", "30s"
    /// - Plural: "2 hours", "10 days"
    pub fn parse(s: &str) -> Result<Self, IntervalParseError> {
        let s = s.trim();

        // Try short format: "1h", "5m", "30s", "7d", "1w"
        if let Some(result) = Self::try_parse_short(s) {
            return result;
        }

        // Try full format: "1 hour", "5 minutes"
        let parts: Vec<&str> = s.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(IntervalParseError::InvalidFormat(s.to_string()));
        }

        let value: i64 = parts[0]
            .parse()
            .map_err(|_| IntervalParseError::InvalidNumber(parts[0].to_string()))?;

        let unit = parts[1].trim().to_lowercase();
        match unit.as_str() {
            "microsecond" | "microseconds" | "us" => Ok(Self::Microseconds(value)),
            "millisecond" | "milliseconds" | "ms" => Ok(Self::Milliseconds(value)),
            "second" | "seconds" | "sec" | "secs" => Ok(Self::Seconds(value)),
            "minute" | "minutes" | "min" | "mins" => Ok(Self::Minutes(value)),
            "hour" | "hours" | "hr" | "hrs" => Ok(Self::Hours(value)),
            "day" | "days" => Ok(Self::Days(value)),
            "week" | "weeks" => Ok(Self::Weeks(value)),
            "month" | "months" | "mon" | "mons" => Ok(Self::Months(value)),
            _ => Err(IntervalParseError::UnknownUnit(unit)),
        }
    }

    fn try_parse_short(s: &str) -> Option<Result<Self, IntervalParseError>> {
        // Must end with a letter and have digits before it
        let boundary = s.find(|c: char| c.is_ascii_alphabetic())?;
        if boundary == 0 {
            return None;
        }

        let (num_str, unit_str) = s.split_at(boundary);

        // Only match single-char short codes to avoid ambiguity with full format
        if unit_str.len() > 2 || unit_str.contains(' ') {
            return None;
        }

        let value: i64 = match num_str.parse() {
            Ok(v) => v,
            Err(_) => return Some(Err(IntervalParseError::InvalidNumber(num_str.to_string()))),
        };

        match unit_str {
            "us" => Some(Ok(Self::Microseconds(value))),
            "ms" => Some(Ok(Self::Milliseconds(value))),
            "s" => Some(Ok(Self::Seconds(value))),
            "m" => Some(Ok(Self::Minutes(value))),
            "h" => Some(Ok(Self::Hours(value))),
            "d" => Some(Ok(Self::Days(value))),
            "w" => Some(Ok(Self::Weeks(value))),
            "M" => Some(Ok(Self::Months(value))),
            _ => None,
        }
    }

    /// Return the SQL interval literal string for use in TimescaleDB functions.
    pub fn to_sql_interval(&self) -> String {
        self.to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntervalParseError {
    InvalidFormat(String),
    InvalidNumber(String),
    UnknownUnit(String),
}

impl fmt::Display for IntervalParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat(s) => write!(f, "invalid interval format: '{s}'"),
            Self::InvalidNumber(s) => write!(f, "invalid number in interval: '{s}'"),
            Self::UnknownUnit(s) => write!(f, "unknown interval unit: '{s}'"),
        }
    }
}

impl std::error::Error for IntervalParseError {}

/// Sort direction for compression ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SortDirection {
    Asc,
    Desc,
}

impl fmt::Display for SortDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Asc => write!(f, "ASC"),
            Self::Desc => write!(f, "DESC"),
        }
    }
}

/// Configuration for creating a hypertable.
#[derive(Debug, Clone)]
pub struct HypertableConfig {
    pub table_name: String,
    pub time_column: String,
    pub chunk_interval: Option<Interval>,
    pub if_not_exists: bool,
}

/// Configuration for enabling compression on a hypertable.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    pub segment_by: Vec<String>,
    pub order_by: Vec<(String, SortDirection)>,
    pub compress_after: Interval,
}

/// Configuration for a continuous aggregate view.
#[derive(Debug, Clone)]
pub struct ContinuousAggregateConfig {
    pub view_name: String,
    pub bucket_interval: Interval,
    pub refresh_policy: Option<RefreshPolicy>,
}

/// Refresh policy for continuous aggregates.
#[derive(Debug, Clone)]
pub struct RefreshPolicy {
    pub start_offset: Interval,
    pub end_offset: Interval,
    pub schedule_interval: Interval,
}

/// Configuration for data retention policies.
#[derive(Debug, Clone)]
pub struct RetentionConfig {
    pub drop_after: Interval,
}
