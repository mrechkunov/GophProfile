package config

import (
	"database/sql"
	"gophprofile/internal/logger"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Config содержит параметры подключения из env
type Config struct {
	Port           string
	MinioHost      string
	MinioUser      string
	MinioPass      string
	MinioSSL       bool
	KafkaBrokers   string
	DBConnStr      string
	MigrationsPath string
}

// Load Config from env
func LoadConfig() Config {
	return Config{
		Port:           getEnv("SERVER_PORT", "8080"),
		MinioHost:      getEnv("MINIO_HOST", "localhost:9000"),
		MinioUser:      getEnv("MINIO_ROOT_USER", "minioadmin"),
		MinioPass:      getEnv("MINIO_ROOT_PASSWORD", "minioadmin"),
		MinioSSL:       getEnv("MINIO_SSL", "false") == "true",
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "localhost:9092"),
		DBConnStr:      getEnv("DATABASE_URI", "postgres://gophprofile_user:secret@postgres/gophprofiledb?sslmode=disable"),
		MigrationsPath: getEnv("MIGRATIONS_PATH", ""),
	}
}

func ConfigureDB(cfg Config) error {
	// create connect to DB and run Up all migrations
	DBconn, err := NewConnect(cfg.DBConnStr)
	if err != nil {
		logger.Log.Errorln("error while connecting to DB (configure service)", err)
		return err
	}
	migrations(DBconn, &cfg)
	err = DBconn.Close()
	if err != nil {
		logger.Log.Errorln("error while close DB connection after migration (configure service)", err)
		return err
	}
	return nil
}

func NewConnect(connString string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		logger.Log.Errorln(err)
	}
	return db, nil
}

func migrations(DBconn *sql.DB, cfg *Config) {
	m, err := migrate.New(
		cfg.MigrationsPath,
		cfg.DBConnStr)
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

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
