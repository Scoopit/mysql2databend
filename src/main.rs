use std::{
    fs::File,
    io::{self, BufRead, BufReader},
};

use clap::{Args, Parser, Subcommand};
use color_eyre::eyre::{Context, Result};
use either::Either;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Opts {
    /// Keep only this database, note that if not database is in the dump
    /// this will have no effect.
    #[arg(short, long, value_parser)]
    databases: Vec<String>,
    /// Keep only this database, note that if not database is in the dump
    /// this will have no effect.
    #[arg(short, long, value_parser)]
    tables: Vec<String>,

    /// Skip USE and CREATE DATABASE statements
    #[arg(short, long, value_parser)]
    skip_database_stmt: bool,

    /// Read data from a file.
    ///
    /// If the filename ends with a .gz extention, try to gunzip...
    #[arg(short, long)]
    input_file: Option<String>,

    /// Allow to control where is output filtered statements
    #[command(subcommand)]
    output: Option<Output>,
}

#[derive(Subcommand, Debug)]
enum Output {
    /// Output data to the standard output (default behaviour)
    Stdout,
    /// Send all data to databend query server API
    Databend(DatabendArgs),
}

#[derive(Args, Debug)]
pub(crate) struct DatabendArgs {
    /// eg. 127.0.0.1:8000/v1/query/    
    #[arg(long, default_value = "http://127.0.0.1:8000/v1/query/")]
    query_uri: String,
    #[arg(long, default_value = "root")]
    user: String,
    #[arg(long)]
    password: Option<String>,
    /// The default database where statements will be executed.
    ///
    /// If during the parsing process a USE statement is encountered the default database
    /// provided with this argument will be overridden.
    #[arg(long)]
    default_database: Option<String>,

    /// Force the database where statements will be executed.
    ///
    /// Use statements will be ignored
    #[arg(long)]
    force_database: Option<String>,
}

fn main() -> Result<()> {
    color_eyre::install()?;
    let args = Opts::parse();

    let mut input: Box<dyn BufRead> = match &args.input_file {
        None => Box::new(io::stdin().lock()),
        Some(file) => {
            if file.ends_with(".gz") {
                Box::new(BufReader::new(flate2::read::GzDecoder::new(File::open(
                    file,
                )?)))
            } else {
                Box::new(BufReader::new(File::open(file)?))
            }
        }
    };

    let mut stdout = io::stdout();

    let mut current_db = None;
    let mut current_table = None;

    let mut line_num = 0;

    let mut buf = Vec::with_capacity(8192);
    let mut parser = parser::Parser::new();

    let mut databend_output = args
        .output
        .map(|output| {
            if let Output::Databend(databend_args) = output {
                Some(databend_output::Output::new(databend_args))
            } else {
                None
            }
        })
        .flatten();

    loop {
        buf.truncate(0);

        line_num += 1;
        input
            .read_until(b'\n', &mut buf)
            .with_context(|| format!("Cannot read dump from stdin at line {line_num}"))?;
        if buf.len() == 0 {
            // EOF
            break;
        }

        match parser.parse(&buf)? {
            parser::StateChange::CreateDatabase(db) => current_db = Some(db),
            parser::StateChange::Table(table) => current_table = Some(table),
            parser::StateChange::None => (),
            parser::StateChange::UseDatabase(db) => {
                databend_output
                    .iter_mut()
                    .for_each(|output| output.set_current_db(&db));
                current_db = Some(db);
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

        // Either implements Write if both sides implement Write, fantastic!
        let mut output = databend_output
            .as_mut()
            .map(Either::Left)
            .unwrap_or_else(|| Either::Right(&mut stdout));

        if !args.skip_database_stmt {
            parser.output_database_statements(&mut output)?;
        }
        if args.tables.len() > 0 {
            // filter by table
            if let Some(current_table) = &current_table {
                if !args.tables.contains(current_table) {
                    continue;
                }
            }
        }
        parser.output_database_content(&mut output)?;
    }

    Ok(())
}
mod databend_output;
mod databend_types;
mod parser;
