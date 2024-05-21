package main

import (
	"flag"
	"github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	pgListen := flag.String("pg_listen", ":5432", "Postgres listen address")
	chListen := flag.String("ch_listen", ":8123", "Clickhouse listen address")
	dbPath := flag.String("db_path", "./test.db", "Path to the database file")
	flag.Parse()
	server := PgServer{}
	err := server.Start(serverOptions{
		DbPath: *dbPath,
		Listen: *pgListen,
		ClickhouseOptions: ClickhouseOptions{
			Enabled: true,
			Listen:  *chListen,
		},
	})
	logrus.Fatal(err)
}
