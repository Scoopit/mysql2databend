use std::io::{self, BufRead};

use clap::Parser;
use color_eyre::eyre::{Context, Result};

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Keep only this database, note that if not database is in the dump
    /// this will have no effect.
    #[clap(short, long, value_parser)]
    databases: Vec<String>,
    /// Keep only this database, note that if not database is in the dump
    /// this will have no effect.
    #[clap(short, long, value_parser)]
    tables: Vec<String>,

    /// Skip USE and CREATE DATABASE statements
    #[clap(short, long, value_parser)]
    skip_database_stmt: bool,
}
fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Args::parse();

    let mut stdin = io::stdin().lock();
    let stdout = io::stdout();

    let mut current_db = None;
    let mut current_table = None;

    let mut line_num = 0;

    let mut buf = Vec::with_capacity(8192);
    let mut parser = parser::Parser::new();
    loop {
        buf.truncate(0);

        line_num += 1;
        stdin
            .read_until(b'\n', &mut buf)
            .with_context(|| format!("Cannot read dump from stdin at line {line_num}"))?;
        if buf.len() == 0 {
            // EOF
            break;
        }

        match parser.parse(&buf)? {
            parser::StateChange::Database(db) => current_db = Some(db),
            parser::StateChange::Table(table) => current_table = Some(table),
            parser::StateChange::None => (),
        }

        if args.databases.len() > 0 {
            // filter by database
            if let Some(current_db) = &current_db {
                if !args.databases.contains(current_db) {
                    continue;
                }
            }
        }
        if !args.skip_database_stmt {
            parser.output_database_statements(&stdout)?;
        }
        if args.tables.len() > 0 {
            // filter by table
            if let Some(current_table) = &current_table {
                if !args.tables.contains(current_table) {
                    continue;
                }
            }
        }

        parser.output_database_content(&stdout)?;
    }

    Ok(())
}
mod parser;
