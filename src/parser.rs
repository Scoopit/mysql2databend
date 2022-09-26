#![allow(unstable_name_collisions)]
use std::io::Write;

use itertools::Itertools;
use lazy_static::lazy_static;
use regex::bytes::{Regex, RegexBuilder};

pub struct Parser {
    buf: Vec<u8>,
    current: Stmt,
}

enum Stmt {
    InCreateTable,
    CreateTable,
    CreateDatabase,
    Use,
    InsertInto,
    // this will be discarded
    Other,
}

pub enum StateChange {
    Database(String),
    Table(String),
    None,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(8129),
            current: Stmt::Other,
        }
    }

    pub fn parse(&mut self, line: &[u8]) -> color_eyre::Result<StateChange> {
        if let Stmt::InCreateTable = self.current {
            if line[0] == b')' {
                // end current create table
                self.current = Stmt::CreateTable;

                convert_to_databend_create_table(&mut self.buf);
            } else {
                // collect content of the create table statement;
                self.buf.extend_from_slice(line);
            }
            Ok(StateChange::None)
        } else {
            self.buf.truncate(0);
            self.buf.extend_from_slice(line);

            // one line statements (or ignorable ones)
            if let Some(use_stmt) = CREATE_DB.captures(line) {
                self.current = Stmt::CreateDatabase;
                let db = String::from_utf8(use_stmt.get(1).unwrap().as_bytes().to_vec())?;

                Ok(StateChange::Database(db))
            } else if let Some(create_stmt) = CREATE.captures(&line) {
                self.current = Stmt::InCreateTable;
                let table = String::from_utf8(create_stmt.get(1).unwrap().as_bytes().to_vec())?;
                Ok(StateChange::Table(table))
            } else if USE.is_match(line) {
                self.current = Stmt::Use;
                Ok(StateChange::None)
            } else if INSERT.is_match(line) {
                self.current = Stmt::InsertInto;
                Ok(StateChange::None)
            } else {
                self.current = Stmt::Other;
                Ok(StateChange::None)
            }
        }
    }

    pub fn output_database_statements<W: Write>(&self, mut out: W) -> std::io::Result<()> {
        match self.current {
            Stmt::CreateDatabase => out.write_all(&self.buf),
            Stmt::Use => out.write_all(&self.buf),
            _ => Ok(()),
        }
    }

    pub fn output_database_content<W: Write>(&self, mut out: W) -> std::io::Result<()> {
        match self.current {
            Stmt::InsertInto | Stmt::CreateTable => out.write_all(&self.buf),
            _ => Ok(()),
        }
    }
}

fn replace(buf: &mut Vec<u8>, regex: &Regex, capture_group: usize, replacement: &[u8]) {
    for capture_range in regex
        .captures_iter(buf)
        .map(|capture| capture.get(capture_group).unwrap().range())
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        buf.splice(capture_range, replacement.iter().copied());
    }
}

fn lower_case_columns(buf: &mut Vec<u8>) {
    for capture_range in COLUMN
        .captures_iter(buf)
        .map(|capture| capture.get(1).unwrap().range())
        .collect::<Vec<_>>()
    {
        let column = &mut buf[capture_range];
        let lower_cased = column.to_ascii_lowercase();
        for i in 0..column.len() {
            column[i] = lower_cased[i];
        }
    }
}

fn convert_to_databend_create_table(buf: &mut Vec<u8>) {
    // rewrite our buffer
    replace(buf, &KEYS_OR_CONTRAINTS, 0, &[]);
    replace(buf, &COLLATE, 0, &[]);
    replace(buf, &CHARSET, 0, &[]);
    replace(buf, &DEFAULT_NULL, 0, b"NULL");
    replace(buf, &DEF_CUR_TS, 0, &[]);
    replace(buf, &ON_UPDATE, 0, &[]);

    // fields without NOT NULL nor NULL must be qualified as null because:
    // - mysql treat them as nullable
    // - databend treat them as not nullable
    // lol
    replace(buf, &NULLABLE, 1, b" NULL");

    lower_case_columns(buf);

    // remove empty lines
    *buf = buf
        .split(|b| *b == b'\n')
        .filter(|sub| !sub.is_empty())
        .intersperse(b"\n")
        .flatten()
        .copied()
        .collect();
    // remove last comma
    if buf[buf.len() - 1] == b',' {
        buf.truncate(buf.len() - 1);
    }
    buf.extend(b"\n);\n");
}

lazy_static! {
    // insert into
    static ref INSERT: Regex  = Regex::new("^INSERT INTO `([^`]+)`").unwrap();
    // database related stuff
    static ref USE: Regex  = Regex::new("^USE `([^`]+)`").unwrap();
    static ref CREATE_DB: Regex = Regex::new("^CREATE DATABASE .*`([^`]+)`").unwrap();
    // create table
    static ref CREATE: Regex = Regex::new("^CREATE TABLE `([^`]+)`").unwrap();
    static ref KEYS_OR_CONTRAINTS: Regex  = RegexBuilder::new("^ +[A-Z]+.*$")
        .multi_line(true)
        .build()
        .unwrap();
    static ref COLLATE: Regex = Regex::new("COLLATE +[a-z0-9_]+").unwrap();
    static ref CHARSET: Regex = Regex::new("CHARACTER SET [a-z0-9_]+").unwrap();
    static ref DEFAULT_NULL: Regex = Regex::new("(NULL )?DEFAULT NULL").unwrap();

    static ref DEF_CUR_TS: Regex = Regex::new("DEFAULT CURRENT_TIMESTAMP").unwrap();
    static ref ON_UPDATE: Regex = Regex::new("ON UPDATE [^, ]+").unwrap();

    // `column` type ,
    static ref NULLABLE: Regex =  Regex::new("`[^`]+` [^ ]+( ?),").unwrap();

    static ref COLUMN: Regex = Regex::new("`([^`]+)` [^(]").unwrap();
}

#[cfg(test)]
mod test {
    use crate::parser::convert_to_databend_create_table;

    #[test]
    fn test() {
        let table = r#"CREATE TABLE `moderated_theme` (
            `theme_lid` bigint(20) NOT NULL DEFAULT '0',
            `state` varchar(32) COLLATE utf8_bin NOT NULL,
            `creationDate` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            `modificationDate` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
            `moderator_lid` bigint(20) DEFAULT NULL,
            `reason` text COLLATE utf8_bin,
            `reviewAskedDate` timestamp NULL DEFAULT NULL,
            `reviewUserText` varchar(1024) COLLATE utf8_bin DEFAULT NULL,
            `reviewText` text COLLATE utf8_bin,
            `greyListNotificationEmailSentDate` timestamp NULL DEFAULT NULL,
            `greyListBlockedNotificationEmailSentDate` timestamp NULL DEFAULT NULL,
            PRIMARY KEY (`theme_lid`),
            KEY `state` (`state`,`creationDate`),
            KEY `state_2` (`state`,`modificationDate`),
            KEY `moderator_lid` (`moderator_lid`),
            KEY `theme_lid` (`theme_lid`,`state`),
            CONSTRAINT `spam_account_ibfk_1` FOREIGN KEY (`theme_lid`) REFERENCES `theme` (`lid`) ON DELETE CASCADE,
            CONSTRAINT `spam_account_ibfk_3` FOREIGN KEY (`moderator_lid`) REFERENCES `user` (`lid`) ON DELETE CASCADE"#;
        let mut buf: Vec<u8> = table.as_bytes().iter().copied().collect();
        convert_to_databend_create_table(&mut buf);
        let converted = String::from_utf8(buf).unwrap();

        println!("{converted}");
    }
}
