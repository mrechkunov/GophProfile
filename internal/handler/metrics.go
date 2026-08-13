package handler

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Структура для хранения инструментов метрик
type AvatarMetrics struct {
	UploadCounter      metric.Int64Counter     // Счетчик попыток загрузки аватаров
	FileSizeHist       metric.Float64Histogram // Гистограмма размеров загружаемых файлов (в КБ/МБ)
	IndexViewsCounter  metric.Int64Counter     // Счетчик просмотров главной страницы
	HealthLatencyDB    metric.Float64Histogram // Задержка пинга БД в мс
	HealthLatencyS3    metric.Float64Histogram // Задержка проверки бакета S3 в мс
	HealthLatencyKafka metric.Float64Histogram // Задержка пинга Kafka в мс
	DownloadCounter    metric.Int64Counter     // Счетчик скачиваний картинок
	DownloadSizeHist   metric.Float64Histogram // Гистограмма размера отдаваемых файлов в байтах
	MetadataCounter    metric.Int64Counter     // Счетчик запросов метаданных
	DeleteCounter      metric.Int64Counter     // Счетчик запросов на удаление
	DeleteDurationHist metric.Float64Histogram // Время выполнения удаления в мс
}

// Инициализация метрик сервиса
func NewAvatarMetrics() (*AvatarMetrics, error) {
	// Получаем глобальный Meter для нашего сервиса
	meter := otel.GetMeterProvider().Meter("gophprofile/handlers")

	uploadCounter, err := meter.Int64Counter(
		"avatar_uploads_total",
		metric.WithDescription("Общее количество загрузок аватаров"),
		metric.WithUnit("{upload}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload counter: %w", err)
	}

	fileSizeHist, err := meter.Float64Histogram(
		"avatar_file_size_bytes",
		metric.WithDescription("Распределение размеров загружаемых аватаров в байтах"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create file size histogram: %w", err)
	}

	indexViewsCounter, err := meter.Int64Counter(
		"avatar_index_views_total",
		metric.WithDescription("Общее количество посещений главной страницы"),
		metric.WithUnit("{view}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create index views counter: %w", err)
	}

	healthLatencyDB, err := meter.Float64Histogram(
		"avatar_health_db_latency_milliseconds",
		metric.WithDescription("Время отклика PostgreSQL во время health check"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create db health metric: %w", err)
	}

	healthLatencyS3, err := meter.Float64Histogram(
		"avatar_health_s3_latency_milliseconds",
		metric.WithDescription("Время отклика MinIO S3 во время health check"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 health metric: %w", err)
	}

	healthLatencyKafka, err := meter.Float64Histogram(
		"avatar_health_kafka_latency_milliseconds",
		metric.WithDescription("Время отклика Kafka во время health check"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka health metric: %w", err)
	}

	downloadCounter, err := meter.Int64Counter(
		"avatar_downloads_total",
		metric.WithDescription("Общее количество запросов на скачивание изображений аватаров"),
		metric.WithUnit("{download}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create download counter: %w", err)
	}

	downloadSizeHist, err := meter.Float64Histogram(
		"avatar_download_size_bytes",
		metric.WithDescription("Размер отправленных пользователю аватаров в байтах"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create download size histogram: %w", err)
	}

	metadataCounter, err := meter.Int64Counter(
		"avatar_metadata_requests_total",
		metric.WithDescription("Общее количество запросов метаданных аватаров"),
		metric.WithUnit("{request}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metadata counter: %w", err)
	}

	deleteCounter, err := meter.Int64Counter(
		"avatar_deletions_total",
		metric.WithDescription("Общее количество запросов на удаление аватаров"),
		metric.WithUnit("{deletion}"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete counter: %w", err)
	}

	deleteDurationHist, err := meter.Float64Histogram(
		"avatar_delete_duration_milliseconds",
		metric.WithDescription("Время обработки операции удаления аватара"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create delete duration histogram: %w", err)
	}

	return &AvatarMetrics{
		UploadCounter:      uploadCounter,
		FileSizeHist:       fileSizeHist,
		IndexViewsCounter:  indexViewsCounter,
		HealthLatencyDB:    healthLatencyDB,
		HealthLatencyS3:    healthLatencyS3,
		HealthLatencyKafka: healthLatencyKafka,
		DownloadCounter:    downloadCounter,
		DownloadSizeHist:   downloadSizeHist,
		MetadataCounter:    metadataCounter,
		DeleteCounter:      deleteCounter,
		DeleteDurationHist: deleteDurationHist,
	}, nil
}
