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

## License

Licensed under either of

- Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license
   ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
