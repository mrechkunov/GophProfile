package config

import (
	"context"
	"database/sql"
	"fmt"

	"gophprofile/internal/logger"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
)

const KafkaResizeTopic = "avatar-resize-tasks"
const KafkaDeleteTopic = "avatar-delete-tasks"

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

type Connections struct {
	DB            *sql.DB
	MinioClient   *minio.Client
	KafkaProducer *kafka.Writer
}

var ConnServer Connections
var ConnWorker Connections
var CfgServer Config
var CfgWorker Config

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

// Load Config from env
func LoadConfig() Config {
	return Config{
		Port:           getEnv("SERVER_PORT", ":8080"),
		MinioHost:      getEnv("MINIO_HOST", "localhost:9000"),
		MinioUser:      getEnv("MINIO_ROOT_USER", "gophprofile_user"),
		MinioPass:      getEnv("MINIO_ROOT_PASSWORD", "supersecretpassword"),
		MinioSSL:       getEnv("MINIO_SSL", "false") == "true",
		KafkaBrokers:   getEnv("KAFKA_BROKERS", "localhost:9092"),
		DBConnStr:      getEnv("DATABASE_URI", "postgres://gophprofile_user:secret@localhost/gophprofiledb?sslmode=disable"),
		MigrationsPath: getEnv("MIGRATIONS_PATH", "file://migrations"),
	}
}

func NewDBConnect(ctx context.Context, connString string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		logger.Log.ErrorContext(ctx, err.Error())
	}
	return db, nil
}

func configureDB(ctx context.Context, cfg Config) (*sql.DB, error) {
	// create connect to DB and run Up all migrations
	dbConn, err := NewDBConnect(ctx, cfg.DBConnStr)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while connecting to DB (configure service)", "err", err)
		return nil, err
	}
	migrations(ctx, dbConn, &cfg)
	return dbConn, nil
}

func migrations(ctx context.Context, dbConn *sql.DB, cfg *Config) {
	m, err := migrate.New(
		cfg.MigrationsPath,
		cfg.DBConnStr)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error initializing migrate:", "err", err)
	}
	// Apply all available migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		logger.Log.ErrorContext(ctx, "error applying migrations:", "err", err)
	}
	logger.Log.InfoContext(ctx, "database migrations applied successfully!")
	err = dbConn.Ping()
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while ping DB after migratioans applied", "err", err)
	}
}

func configureMinIO(ctx context.Context, cfg Config) (*minio.Client, error) {
	// MinIO конфигурируем
	minioClient, err := minio.New(cfg.MinioHost, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioUser, cfg.MinioPass, ""),
		Secure: cfg.MinioSSL,
	})
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while minio client creating:", "err", err)
		return nil, err
	}
	// Создание бакета
	bucketName := "avatars"
	// Проверяем, существует ли уже бакет
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while bucket test:", "err", err)
		return nil, err
	}

	if !exists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			logger.Log.ErrorContext(ctx, "error while bucket creating:", "err", err)
			return nil, err
		}
		logger.Log.InfoContext(ctx, "bucket is created sucsessfully!", "bucketName", bucketName)
	} else {
		logger.Log.InfoContext(ctx, "bucket is already exist.", "bucketName", bucketName)
	}
	return minioClient, nil
}
func configureKafka(ctx context.Context, cfg Config) (*kafka.Writer, error) {
	brokerAddress := cfg.KafkaBrokers

	// Подключаемся к любому брокеру, чтобы найти контроллер
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddress)
	if err != nil {
		logger.Log.ErrorContext(ctx, err.Error())
		return nil, err
	}
	defer conn.Close()

	// Получаем адрес текущего контроллера для выполнения операций администрирования
	controller, err := conn.Controller()
	if err != nil {
		logger.Log.ErrorContext(ctx, err.Error())
		return nil, err
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		logger.Log.ErrorContext(ctx, err.Error())
		return nil, err
	}
	defer controllerConn.Close()

	// Создаем конфигурацию топика
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             KafkaResizeTopic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
		{
			Topic:             KafkaDeleteTopic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		},
	}
	// Отправляем запрос на создание
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		logger.Log.ErrorContext(ctx, err.Error())
		return nil, err
	}
	logger.Log.InfoContext(ctx, "Topics are created sucsessfuly!", KafkaResizeTopic, KafkaResizeTopic)

	// Настройка продюсера (Writer)
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers), // Адрес вашего Kafka-брокера
		Balancer: &kafka.LeastBytes{},         // Алгоритм распределения по партициям
	}
	return writer, nil
}

func InitServer(ctx context.Context) {
	CfgServer = LoadConfig()
	var err error
	// DB конфигурируем
	ConnServer.DB, err = configureDB(ctx, CfgServer)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while db configure", "err", err)
	}
	ConnServer.MinioClient, err = configureMinIO(ctx, CfgServer)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while minIO configure", "err", err)
	}
	ConnServer.KafkaProducer, err = configureKafka(ctx, CfgServer)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while kafka configure", "err", err)
	}
}
func InitWorker(ctx context.Context) {
	CfgWorker = LoadConfig()
	var err error
	// create connect to DB
	ConnWorker.DB, err = NewDBConnect(ctx, CfgWorker.DBConnStr)
	if err != nil {
		logger.Log.ErrorContext(context.Background(), "error while connecting to DB (configure service)")
		return
	}
	// MinIO конфигурируем
	ConnWorker.MinioClient, err = minio.New(CfgWorker.MinioHost, &minio.Options{
		Creds:  credentials.NewStaticV4(CfgWorker.MinioUser, CfgWorker.MinioPass, ""),
		Secure: CfgWorker.MinioSSL,
	})
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while minio client creating:", "err", err)
		return
	}
	// Настройка продюсера
	ConnWorker.KafkaProducer = &kafka.Writer{
		Addr:     kafka.TCP(CfgWorker.KafkaBrokers),
		Balancer: &kafka.LeastBytes{},
	}
}
