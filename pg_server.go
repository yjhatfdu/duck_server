package main

import (
	"database/sql"
	"github.com/marcboeker/go-duckdb"
	"github.com/sirupsen/logrus"
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
}

type PgServer struct {
	Connector *duckdb.Connector
	conn      *sql.DB
	backends  sync.Map
}

func (s *PgServer) Start(options serverOptions) error {
	duckConnector, err := duckdb.NewConnector(options.DbPath, nil)
	if err != nil {
		return err
	}
	logrus.Infof("Open DuckDB database at %s", options.DbPath)
	s.Connector = duckConnector
	s.conn = sql.OpenDB(s.Connector)
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

func (s *PgServer) StartClickhouseHttp(options ClickhouseOptions) {
	chServer := ChServer{conn: sql.OpenDB(s.Connector), connector: s.Connector}
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
