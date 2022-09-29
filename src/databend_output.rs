use std::{
    error::Error,
    io::{self, Write},
};

use reqwest::Url;

use crate::{databend_types::ExecuteStateKind, DatabendArgs};

pub(crate) struct Output {
    /// current database issued by `USE` keywords
    database: Option<String>,

    config: super::DatabendArgs,
}

impl Output {
    pub(crate) fn new(config: DatabendArgs) -> Self {
        Self {
            // init current db with default db
            database: config.default_database.clone(),
            config,
        }
    }

    pub(crate) fn set_current_db(&mut self, db: &str) {
        self.database = Some(db.into());
    }
}

fn to_io_error<E: Into<Box<dyn Error + Send + Sync>>>(error: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error)
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let response = reqwest::blocking::Client::default()
            .post(
                self.config
                    .query_uri
                    .parse::<Url>()
                    .expect("Cannot parse query_uri")
                    .join("/v1/query")
                    .unwrap(),
            )
            .basic_auth(&self.config.user, self.config.password.as_ref())
            .json(&crate::databend_types::HttpQueryRequest {
                session: self
                    .config
                    .force_database
                    .as_ref()
                    .or(self.config.default_database.as_ref())
                    .map(|db| crate::databend_types::HttpSessionConf {
                        database: Some(db.to_string()),
                        ..Default::default()
                    }),
                sql: String::from_utf8(buf.to_vec()).map_err(to_io_error)?,
                pagination: Some(crate::databend_types::PaginationConf {
                    wait_time_secs: Some(120),
                    ..Default::default()
                }),
                string_fields: None,
                session_id: None,
            })
            .send()
            .map_err(to_io_error)?
            .error_for_status()
            .map_err(to_io_error)?
            .json::<crate::databend_types::QueryResponse>()
            .map_err(to_io_error)?;

        match response.state {
            ExecuteStateKind::Running => eprintln!("Query still running\n{:#?}", response),
            ExecuteStateKind::Failed => panic!("Query failed!\n{:#?}", response),
            ExecuteStateKind::Succeeded => eprintln!(
                "Written {} rows ({} bytes) in {}ms",
                response.stats.progresses.write_progress.rows,
                response.stats.progresses.write_progress.bytes,
                response.stats.running_time_ms,
            ),
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
