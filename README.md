mysql2databend
==============

Quick & Dirtyl CLI to process mysql dumps and clean them so they can be ingested
in Databend using a regular MySQL client.

Features:

- read from `stdin`, output to `stdout`
- table filtering
- database filtering
- rename table fields to lower case (databend seems to struggle with fields name like `registrationDate`)
- modify `CREATE TABLE` statements (remove collation, transforms `DEFAULT NULL` to `NULL`)
- filter out anything except `USE`, `CREATE TABLE`, `CREATE DATABASE` and `INSERT INTO`
- optionnally filter out `USE` and `CREATE TABLE` statements
