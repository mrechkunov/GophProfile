package config

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"gophprofile/internal/logger"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/segmentio/kafka-go"
)

const KafkaResizeTopic = "avatar-resize-tasks"
const KafkaDeleteTopic = "avatar-delete-tasks"

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

// NewDBConnect инициализирует пул с жесткими Production-лимитами ресурсов
func NewDBConnect(ctx context.Context, connString string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		logger.Log.ErrorContext(ctx, "failed to open database pool", "err", err)
		return nil, err
	}

	// настройка лимита ресурсного пулла (Защита СУБД от падения по OOM)
	db.SetMaxOpenConns(25)                 // Максимум одновременных активных подключений
	db.SetMaxIdleConns(25)                 // Сколько простаивающих коннектов держать готовыми
	db.SetConnMaxLifetime(5 * time.Minute) // Защита от старения сокетов и утечек в ОС
	db.SetConnMaxIdleTime(2 * time.Minute) // Сворачивать лишние коннекты при спаде нагрузки

	return db, nil
}

func configureDB(ctx context.Context, cfg Config) (*sql.DB, error) {
	dbConn, err := NewDBConnect(ctx, cfg.DBConnStr)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while connecting to DB (configure service)", "err", err)
		return nil, err
	}

	// Запускаем миграции, используя созданный dbConn
	err = migrations(ctx, &cfg)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while migration apply in DB", "err", err)
		return nil, err
	}
	return dbConn, nil
}

func migrations(ctx context.Context, cfg *Config) error {
	dbConn, err := NewDBConnect(ctx, cfg.DBConnStr)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while connecting to DB (configure service migration)", "err", err)
		return err
	}
	driver, err := postgres.WithInstance(dbConn, &postgres.Config{})
	if err != nil {
		logger.Log.ErrorContext(ctx, "error creating migration db driver", "err", err)
		return err
	}

	m, err := migrate.NewWithDatabaseInstance(cfg.MigrationsPath, "postgres", driver)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error initializing migrate instance", "err", err)
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		logger.Log.ErrorContext(ctx, "error applying migrations", "err", err)
		return err
	}

	logger.Log.InfoContext(ctx, "database migrations applied successfully!")

	// Валидируем состояние базы данных быстрым пингом
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err = dbConn.PingContext(pingCtx); err != nil {
		logger.Log.ErrorContext(ctx, "error while ping DB after migrations applied", "err", err)
	}
	return nil
}

func configureMinIO(ctx context.Context, cfg Config) (*minio.Client, error) {
	// Ограничиваем сетевое взаимодействие с MinIO на этапе старта
	initCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	minioClient, err := minio.New(cfg.MinioHost, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioUser, cfg.MinioPass, ""),
		Secure: cfg.MinioSSL,
	})
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while minio client creating", "err", err)
		return nil, err
	}

	bucketName := "avatars"
	exists, err := minioClient.BucketExists(initCtx, bucketName)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while bucket test", "err", err)
		return nil, err
	}

	if !exists {
		err = minioClient.MakeBucket(initCtx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			logger.Log.ErrorContext(ctx, "error while bucket creating", "err", err)
			return nil, err
		}
		logger.Log.InfoContext(ctx, "bucket is created successfully!", "bucketName", bucketName)
	} else {
		logger.Log.InfoContext(ctx, "bucket is already exist.", "bucketName", bucketName)
	}
	return minioClient, nil
}

func configureKafka(ctx context.Context, cfg Config) (*kafka.Writer, error) {
	brokerAddress := cfg.KafkaBrokers

	// Ограничиваем общее время развертывания топиков на старте (Защита от зависания пода)
	initCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	dialer := &kafka.Dialer{
		Timeout:   3 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	conn, err := dialer.DialContext(initCtx, "tcp", brokerAddress)
	if err != nil {
		logger.Log.ErrorContext(ctx, "failed to dial kafka broker", "err", err)
		return nil, err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		logger.Log.ErrorContext(ctx, "failed to get kafka controller", "err", err)
		return nil, err
	}

	controllerAddress := net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port))
	controllerConn, err := dialer.DialContext(initCtx, "tcp", controllerAddress)
	if err != nil {
		logger.Log.ErrorContext(ctx, "failed to dial kafka controller node", "err", err)
		return nil, err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{Topic: KafkaResizeTopic, NumPartitions: 3, ReplicationFactor: 1},
		{Topic: KafkaDeleteTopic, NumPartitions: 3, ReplicationFactor: 1},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		var kErr kafka.Error
		if errors.As(err, &kErr) && kErr != kafka.TopicAlreadyExists {
			logger.Log.ErrorContext(ctx, "failed to create kafka topics", "err", err)
			return nil, err
		}
	}
	logger.Log.InfoContext(ctx, "Topics validation completed successfully", "resize", KafkaResizeTopic, "delete", KafkaDeleteTopic)

	// Настройка продюссера с таймаутами
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers),
		Balancer:     &kafka.LeastBytes{},
		ReadTimeout:  5 * time.Second,  // Таймаут на получение подтверждения (ACK) от брокера
		WriteTimeout: 5 * time.Second,  // Таймаут на отправку пачки байт в сетевой сокет
		RequiredAcks: kafka.RequireAll, // Гарантия сохранности данных (ожидаем запись на реплики)
		MaxAttempts:  3,                // Встроенные повторные попытки при сетевых морганиях
	}
	return writer, nil
}

func InitServer(ctx context.Context) {
	CfgServer = LoadConfig()
	var err error

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

	ConnWorker.DB, err = NewDBConnect(ctx, CfgWorker.DBConnStr)
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while connecting to DB (configure worker)", "err", err)
		return
	}

	ConnWorker.MinioClient, err = minio.New(CfgWorker.MinioHost, &minio.Options{
		Creds:  credentials.NewStaticV4(CfgWorker.MinioUser, CfgWorker.MinioPass, ""),
		Secure: CfgWorker.MinioSSL,
	})
	if err != nil {
		logger.Log.ErrorContext(ctx, "error while minio client creating for worker", "err", err)
		return
	}

	// Добавлены таймауты для продюсера на стороне воркера
	ConnWorker.KafkaProducer = &kafka.Writer{
		Addr:         kafka.TCP(CfgWorker.KafkaBrokers),
		Balancer:     &kafka.LeastBytes{},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		MaxAttempts:  3,
	}
}
