package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
	"github.com/supercaracal/scram-sha-256/pkg/pgpasswd"
	"net"
	"net/http"
	"sync"
)

type ClickhouseOptions struct {
	Enabled bool
	Listen  string
}

type serverOptions struct {
	DbPath            string
	Listen            string
	ClickhouseOptions ClickhouseOptions
	UseHack           bool
	Auth              bool
}

type PgServer struct {
	Connector  *duckdb.Connector
	conn       *sql.DB
	backends   sync.Map
	enableAuth bool
}

func duckdbInit(execer driver.ExecerContext) error {
	var statements = []string{
		`create view if not exists pg_type as select type_oid as oid,case when logical_type like '%TIMESTAMP_%' then 'TIMESTAMP' when logical_type = 'DECIMAL' then 'NUMERIC' when logical_type='BOOLEAN' then 'bool' else logical_type end as typname from duckdb_types where oid is not null;`,
		`create view if not exists pg_matviews as select '' as  matviewname , '' as schemaname limit 0;`,
		`create view if not exists information_schema.constraint_column_usage as select '' constraint_name limit 0;`,
		`create function if not exists array_positions(a,b) as 0;`,
		`create function if not exists timezone() as 'utc';`,
		`create function if not exists currentDatabase() as current_schema();`,
		`create schema if not exists system;`,
		`create view if not exists system.databases as
select schema_name as name
from information_schema.schemata
where catalog_name not in ('system', 'temp');`,
		`create view if not exists system.tables as
select table_name    as name,
       table_schema  as database,
       'uuid'        as uuid,
       'duckdb'      as engine,
       0             as is_temporary,
       table_comment as comment
from information_schema.tables
where table_type = 'BASE TABLE';`,
		`create view if not exists system.columns as
select table_schema   as database,
       table_name     as table,
       column_name    as name,
       data_type      as type,
       column_comment as comment,
       data_type         default_kind,
       column_default as default_expression
from information_schema.columns;`,
		`create view if not exists system.functions as
select proname as name, prokind = 'a' as is_aggregate
from pg_proc;`,
	}
	for _, stmt := range statements {
		if _, err := execer.ExecContext(context.Background(), stmt, nil); err != nil {
			return err
		}
	}
	return nil
}

func (s *PgServer) Start(options serverOptions) error {
	var duckConnector *duckdb.Connector
	var err error
	if options.UseHack {
		duckConnector, err = duckdb.NewConnector(options.DbPath, duckdbInit)
	} else {
		duckConnector, err = duckdb.NewConnector(options.DbPath, nil)
	}
	if err != nil {
		return err
	}
	logrus.Infof("Open DuckDB database at %s", options.DbPath)
	s.Connector = duckConnector
	s.conn = sql.OpenDB(s.Connector)

	if options.Auth {
		s.enableAuth = true
		_, err = s.conn.ExecContext(context.Background(), "create schema if not exists duckserver;")
		_, err = s.conn.ExecContext(context.Background(), "create table if not exists duckserver.users (username text primary key, password text);")
	}
	if options.ClickhouseOptions.Enabled {
		go s.StartClickhouseHttp(options.ClickhouseOptions)
	}
	lis, err := net.Listen("tcp", options.Listen)
	if err != nil {
		return err
	}
	logrus.Infof("Listening postgresql wire protocol on %s", options.Listen)
	for {
		conn, err := lis.Accept()
		if err != nil {
			continue
		}
		pgConn := newPgConn(conn, s)
		pgConn.Run()
	}
}

func (s *PgServer) CreateUser(user, password string) error {
	pass, err := pgpasswd.Encrypt([]byte(password))
	if err != nil {
		return err
	}
	_, err = s.conn.ExecContext(context.Background(), "insert into duckserver.users (username, password) values ($1, $2)", user, pass)
	return err
}

func (s *PgServer) GetPassword(user string) (string, error) {
	var pass string
	err := s.conn.QueryRowContext(context.Background(),
		"select password from duckserver.users where username = $1", user).Scan(&pass)
	return pass, err
}

func (s *PgServer) StartClickhouseHttp(options ClickhouseOptions) {
	chServer := ChServer{conn: sql.OpenDB(s.Connector), connector: s.Connector, pgServer: s}
	logrus.Infof("Listening clickhouse http protocol on %s", options.Listen)
	logrus.Fatal(http.ListenAndServe(options.Listen, &chServer))
}

func (s *PgServer) Close(key [8]byte) {
	s.backends.Delete(key)
}

func (s *PgServer) CancelRequest(key [8]byte) {
	if backend, ok := s.backends.Load(key); ok {
		if backend.(*PgConn).cancel != nil {
			backend.(*PgConn).cancel()
		}
	}
}
