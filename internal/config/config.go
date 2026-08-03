package config

import (
	"database/sql"
	"flag"
	"gophprofile/internal/logger"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// DB connection
var DBconn *sql.DB

type Addresses struct {
	ServerBindAddress string
	MigrationsPath    string
	DBConnStr         string
}

var ConfigAddresses = Addresses{
	ServerBindAddress: "localhost:8080",
	MigrationsPath:    "",
	DBConnStr:         "",
}

func Init() {
	ba := flag.String("a", "localhost:8080", "adress to server run")
	mp := flag.String("m", "file://migrations", "default migration PATH")
	cs := flag.String("d", "postgres://gophprofile_user:secret@localhost/gophprofiledb?sslmode=disable", "default DBConnStr")
	flag.Parse()

	// если переиенные окружения установленны, берем их, иначе берем флаг
	if serverAddress, isEnvBindSrv := os.LookupEnv("RUN_ADDRESS"); isEnvBindSrv {
		ConfigAddresses.ServerBindAddress = serverAddress
	} else {
		ConfigAddresses.ServerBindAddress = *ba
	}

	if migrationsPath, isEnvMigrationsPath := os.LookupEnv("MIGRATIONS_PATH"); isEnvMigrationsPath {
		ConfigAddresses.MigrationsPath = migrationsPath
	} else {
		ConfigAddresses.MigrationsPath = *mp
	}
	if dbConnStr, isEnvDBConnStr := os.LookupEnv("DATABASE_URI"); isEnvDBConnStr {
		ConfigAddresses.DBConnStr = dbConnStr
	} else {
		ConfigAddresses.DBConnStr = *cs
	}
	// create connect to DB and run Up all migrations
	var err error
	DBconn, err = NewConnect()
	if err != nil {
		logger.Log.Errorln("error while connecting to DB (configure service)", err)
	}
	migrations(DBconn)
}
func NewConnect() (*sql.DB, error) {
	db, err := sql.Open("pgx", ConfigAddresses.DBConnStr)
	if err != nil {
		logger.Log.Errorln(err)
	}
	return db, err
}

func migrations(DBconn *sql.DB) {
	m, err := migrate.New(
		ConfigAddresses.MigrationsPath,
		ConfigAddresses.DBConnStr)
	if err != nil {
		logger.Log.Errorln("error initializing migrate:", err)
	}
	// Apply all available migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logger.Log.Errorln("error applying migrations:", err)
	}
	logger.Log.Infoln("database migrations applied successfully!")
	err = DBconn.Ping()
	if err != nil {
		logger.Log.Warnln("error while ping DB after migratioans applied", err)
	}
}
