#![allow(dead_code)]
//! Datatypes used in databend v1 query api
//!
//! Each struct has been adapted from databend source code
//! to allow setting optional as Options.
//!

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug)]
pub struct HttpQueryRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session: Option<HttpSessionConf>,
    pub sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<PaginationConf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub string_fields: Option<bool>,
}

// for reference purpose
const DEFAULT_MAX_ROWS_IN_BUFFER: usize = 5 * 1000 * 1000;
const DEFAULT_MAX_ROWS_PER_PAGE: usize = 10000;
const DEFAULT_WAIT_TIME_SECS: u32 = 1;

#[derive(Deserialize, Serialize, Debug, Default)]
pub struct PaginationConf {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wait_time_secs: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_rows_in_buffer: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_rows_per_page: Option<usize>,
}

#[derive(Deserialize, Serialize, Debug, Default, PartialEq, Eq, Clone)]
pub struct HttpSessionConf {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_server_session_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct QueryStats {
    #[serde(flatten)]
    pub progresses: Progresses,
    pub running_time_ms: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResponse {
    pub id: String,
    pub session_id: Option<String>,
    pub session: Option<HttpSessionConf>,
    pub schema: Option<serde_json::Value>,
    pub data: Vec<serde_json::Value>,
    pub state: ExecuteStateKind,
    // only sql query error
    pub error: Option<QueryError>,
    pub stats: QueryStats,
    pub affect: Option<serde_json::Value>,
    pub stats_uri: Option<String>,
    // just call it after client not use it anymore, not care about the server-side behavior
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum ExecuteStateKind {
    Running,
    Failed,
    Succeeded,
}

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Progresses {
    pub scan_progress: ProgressValues,
    pub write_progress: ProgressValues,
    pub result_progress: ProgressValues,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ProgressValues {
    pub rows: usize,
    pub bytes: usize,
}
