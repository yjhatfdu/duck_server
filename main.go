package main

import (
	"flag"
	"github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
)

const VERSION = "0.1.0"

func main() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
	logrus.Infof("duck_server %s", VERSION)
	pgListen := flag.String("pg_listen", ":5432", "Postgres listen address")
	chListen := flag.String("ch_listen", ":8123", "Clickhouse listen address")
	dbPath := flag.String("db_path", "./test.db", "Path to the database file")
	logLevel := flag.String("log_level", "info", "Log level")
	hack := flag.Bool("hack", true, "hack")
	auth := flag.Bool("auth", true, "enable auth")
	flag.Parse()
	switch *logLevel {
	case "trace":
		logrus.SetLevel(logrus.TraceLevel)
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
	case "warn":
		logrus.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
	}
	server := PgServer{}
	err := server.Start(serverOptions{
		DbPath:  *dbPath,
		Listen:  *pgListen,
		UseHack: *hack,
		ClickhouseOptions: ClickhouseOptions{
			Enabled: true,
			Listen:  *chListen,
		},
		Auth: *auth,
	})
	logrus.Fatal(err)
}
