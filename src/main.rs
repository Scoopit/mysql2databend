use clap::Parser;
use color_eyre::eyre::{Context, Result};
use lazy_static::lazy_static;
use regex::bytes::Regex;
use std::io::{self, BufRead, Write};

#[derive(PartialEq, Eq)]
enum State {
    InCreateTable(String),
    Other,
}

lazy_static! {
    // any statement
    static ref STMT: Regex = Regex::new("^[A-Z]+ ").unwrap();

    // insert into
    static ref INSERT: Regex  = Regex::new("^INSERT INTO `([^`]+)`").unwrap();
    // database related stuff
    static ref USE: Regex  = Regex::new("^USE `([^`]+)`").unwrap();
    static ref CREATE_DB: Regex = Regex::new("^CREATE DATABASE .*`([^`]+)`").unwrap();
    // create table
    static ref CREATE: Regex = Regex::new("^CREATE TABLE `([^`]+)`").unwrap();
    static ref KEYS_OR_CONTRAINTS: Regex  = Regex::new("^ +[A-Z]+").unwrap();
    static ref COLLATE: Regex = Regex::new("COLLATE +[a-z0-9_]+").unwrap();
    static ref CHARSET: Regex = Regex::new("CHARACTER SET [a-z0-9_]+").unwrap();
    static ref DEFAULT_NULL: Regex = Regex::new("(NULL )?DEFAULT NULL").unwrap();
}

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
    let mut stdout = io::stdout();

    let mut state = State::Other;

    let mut current_db = None;
    let mut current_table = None;

    let mut line_num = 0;

    let mut buf = Vec::with_capacity(8192);
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
        if let State::InCreateTable(_) = &state {
            // Create table terminator
            if buf[0] == b')' {
                write!(stdout, ");\n")?;
                state = State::Other;
            } else if KEYS_OR_CONTRAINTS.is_match(&buf) {
                // Ignore keys & contraints
            } else {
                // TODO rewrite stuff
                if let Some(collate) = COLLATE.captures(&buf) {
                    buf.splice(collate.get(0).unwrap().range(), []);
                }
                if let Some(collate) = CHARSET.captures(&buf) {
                    buf.splice(collate.get(0).unwrap().range(), []);
                }
                if let Some(collate) = DEFAULT_NULL.captures(&buf) {
                    buf.splice(collate.get(0).unwrap().range(), b"NULL".iter().map(|u| *u));
                }

                stdout.write_all(&buf)?;
            }
        } else if STMT.is_match(&buf) {
            // regular statement, anything else will be wiped out.
            if let Some(use_stmt) = CREATE_DB.captures(&buf) {
                let db = String::from_utf8(use_stmt.get(1).unwrap().as_bytes().to_vec())?;
                current_db = Some(db.clone());
                if args.databases.len() == 0 || args.databases.contains(&db) {
                    // only print out create database statement if there are no filter on databases or if the database
                    // in in the filter!
                    stdout.write_all(&buf)?;
                }
            }

            if args.databases.len() > 0 {
                // filter by database
                if let Some(current_db) = &current_db {
                    if !args.databases.contains(current_db) {
                        continue;
                    }
                }
            }

            if USE.is_match(&buf) {
                stdout.write_all(&buf)?;
            }

            if let Some(create_stmt) = CREATE.captures(&buf) {
                let table = String::from_utf8(create_stmt.get(1).unwrap().as_bytes().to_vec())?;
                state = State::InCreateTable(table.clone());
                current_table = Some(table.clone());

                if args.tables.len() > 0 {
                    if !args.tables.contains(&table) {
                        // we need to skip the create table output
                        state = State::Other;
                        continue;
                    }
                }

                stdout.write_all(&buf)?;
            }
            if args.tables.len() > 0 {
                // filter by table
                if let Some(current_table) = &current_table {
                    if !args.tables.contains(current_table) {
                        continue;
                    }
                }
            }
            if INSERT.is_match(&buf) {
                stdout.write_all(&buf)?;
            }
        }
    }

    Ok(())
}
