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

var Conn Connections
var Cfg Config

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

func NewDBConnect(connString string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		logger.Log.Errorln(err)
	}
	return db, nil
}

func configureDB(cfg Config) (*sql.DB, error) {
	// create connect to DB and run Up all migrations
	dbConn, err := NewDBConnect(cfg.DBConnStr)
	if err != nil {
		logger.Log.Errorln("error while connecting to DB (configure service)", err)
		return nil, err
	}
	migrations(dbConn, &cfg)
	return dbConn, nil
}

func migrations(dbConn *sql.DB, cfg *Config) {
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
	err = dbConn.Ping()
	if err != nil {
		logger.Log.Warnln("error while ping DB after migratioans applied", err)
	}
}

func configureMinIO(cfg Config) (*minio.Client, error) {
	// MinIO конфигурируем
	minioClient, err := minio.New(cfg.MinioHost, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinioUser, cfg.MinioPass, ""),
		Secure: cfg.MinioSSL,
	})
	if err != nil {
		logger.Log.Errorln("error while minio client creating:", err)
		return nil, err
	}
	// Создание бакета
	ctx := context.Background()
	bucketName := "avatars"
	// Проверяем, существует ли уже бакет
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		logger.Log.Errorln("error while bucket test:", err)
		return nil, err
	}

	if !exists {
		err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
		if err != nil {
			logger.Log.Errorln("error while bucket creating:", err)
			return nil, err
		}
		logger.Log.Infoln("bucket", bucketName, "is created sucsessfully!")
	} else {
		logger.Log.Infoln("bucket", bucketName, "is already exist.")
	}
	return minioClient, nil
}
func configureKafka(cfg Config) (*kafka.Writer, error) {
	ctx := context.Background()
	brokerAddress := cfg.KafkaBrokers
	topicName := "avatar-resize-tasks"

	// Подключаемся к любому брокеру, чтобы найти контроллер
	conn, err := kafka.DialContext(ctx, "tcp", brokerAddress)
	if err != nil {
		logger.Log.Errorln(err.Error())
		return nil, err
	}
	defer conn.Close()

	// Получаем адрес текущего контроллера для выполнения операций администрирования
	controller, err := conn.Controller()
	if err != nil {
		logger.Log.Errorln(err.Error())
		return nil, err
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		logger.Log.Errorln(err.Error())
		return nil, err
	}
	defer controllerConn.Close()

	// Создаем конфигурацию топика
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topicName,
			NumPartitions:     3,
			ReplicationFactor: 1, // Для локального dev-кластера (в prod обычно >= 3)
		},
	}
	// Отправляем запрос на создание
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		logger.Log.Errorln(err.Error())
		return nil, err
	}
	logger.Log.Infoln("Topic", topicName, "is created sucsessfuly!")

	// Настройка продюсера (Writer)
	writer := &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers), // Адрес вашего Kafka-брокера
		Balancer: &kafka.LeastBytes{},         // Алгоритм распределения по партициям
	}
	return writer, nil
}

func Init() {
	Cfg = LoadConfig()
	var err error
	// DB конфигурируем
	Conn.DB, err = configureDB(Cfg)
	if err != nil {
		logger.Log.Errorln("error while db configure", err)
	}
	Conn.MinioClient, err = configureMinIO(Cfg)
	if err != nil {
		logger.Log.Errorln("error while minIO configure", err)
	}
	Conn.KafkaProducer, err = configureKafka(Cfg)
	if err != nil {
		logger.Log.Errorln("error while kafka configure", err)
	}
}

// // Настройка продюсера (Writer)
// 	writer := &kafka.Writer{
// 		Addr:     kafka.TCP("localhost:9092"), // Адрес вашего Kafka-брокера
// 		Topic:    "my-topic",
// 		Balancer: &kafka.LeastBytes{}, // Алгоритм распределения по партициям
// 	}
// 	defer writer.Close()

// 	ctx := context.Background()

// 	// Отправка сообщения
// 	msg := kafka.Message{
// 		Key:   []byte("key-1"),
// 		Value: []byte("Привет, мир из Go!"),
// 	}

// 	err := writer.WriteMessages(ctx, msg)
// 	if err != nil {
// 		log.Fatalf("Ошибка отправки сообщения: %v\n", err)
// 	}

// 	fmt.Println("Сообщение успешно отправлено в Kafka")
// }
