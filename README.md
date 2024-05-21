# DuckServer

A standalone [DuckDB](https://duckdb.org) server support postgresql wire protocol and clickhouse http protocol. Build
with [DuckDB go driver](https://github.com/marcboeker/go-duckdb).

## Why?

DuckDB is a high performance in-memory OLAP database, but it doesn't support network access. This project is a simple
server that can accept postgresql wire protocol and clickhouse http protocol, and forward the query to DuckDB. It's
useful for testing and integration with other tools.

Compare to Clickhouse, DuckDB has much better query optimizer and support more sql features such as primary key, foreign
key, unique key. DuckDB also support more performant update/delete with transaction support.

## Features

- Almost everything DuckDB supported
- Support concurrent read and write query from multiple clients
- Support postgresql wire protocol(both simple and extended query protocol)
- Support postgresql COPY FROM STDIN for bulk import
- Support clickhouse http protocol
- Support clickhouse select/insert with format TabSeparated/CSV/JSONEachRow
- Optimize bulk load with DuckDB Appender api
- Tested with psql, jackc/pgx, postgres-jdbc, clickhouse-jdbc, curl

## Usage

### Build

```shell
$ go build -o DuckServer
```

### start server

```shell
$ ./DuckServer
```

### start with options

```shell
$ ./DuckServer --pg_listen :5432 --ch_listen :8123 --db_path /tmp/DuckServer
```

### run with docker

```shell
$ docker build -t duck_server .
$ docker run -d -p 5432:5432 -p 8123:8123 -v /tmp/duck_server:/tmp/duck_server --name duck_server duck_server
```

### connect with psql

```shell
$ psql -h 127.0.0.1
$ psql -h 127.0.0.1 -c 'select 1'
```

### use clickhouse http protocol

```shell
$ curl 'http://localhost:8123/?query=SELECT%201'
$ echo 'CREATE TABLE t (a int)' | curl 'http://localhost:8123/' --data-binary @-
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

### bulk load csv

```shell
$ psql -h 127.0.0.1 -c 'COPY tbl from stdin with csv' < data.csv
$ curl -X POST 'http://localhost:8123/?query=INSERT%20INTO%20tbl%20FORMAT%20CSV' -T data.csv
```

## Limitation

- No support for clickhouse TCP protocol, so clickhouse-client doesn't work
- No authentication support for now, so only use in trusted network
- No user and privilege management, DuckDB can execute shell, use with caution
- Some database tools may not work well, like pgAdmin, dbeaver, etc
- Some prepared statement with dynamic type may not work well,
  such as ```select $1``` . because DuckDB describe will return type as 'int', but the actual value is 'unkonwn'.
  some client may not handle this well, like pgx/v5 will throw error. You can use explicit cast ```select $1::text``` to avoid this issue.


## Work in progress

- [ ] A funny logo for DuckServer
- [ ] Support all data types in DuckDB
- [ ] Support postgresql style 'Copy To Stdout'
- [ ] Support SCRAM-SHA-256 authentication for postgresql protocol
- [ ] Support basic auth for clickhouse http protocol
- [ ] Support http compression for clickhouse http protocol
- [ ] Tests for postgresql and clickhouse protocol
- [ ] Documentation for code
- [ ] CI and release build

## Caution⚠️

DuckServer is a hobby project, and just hacked in a weekend. It's not well tested and may have bugs. Use it at your own
risk. If you find any issue, please report it in the issue page.