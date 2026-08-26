package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"gophprofile/internal/config"
	"gophprofile/internal/logger"
	"gophprofile/internal/model"
	"gophprofile/internal/repository"

	"github.com/disintegration/gift"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
	"golang.org/x/image/webp"
)

const (
	KafkaResizeGroupID = "avatar-resize-worker-group"
	KafkaDeleteGroupID = "avatar-delete-cleaner-group"
	BucketName         = "avatars"

	// Ограничение размера файла в памяти для предотвращения OOM (15 МБ)
	MaxImageMemorySize = 15 * 1024 * 1024
)

func init() {
	image.RegisterFormat("webp", "RIFF????WEBP", webp.Decode, webp.DecodeConfig)
}

// ==========================================
// 1. ВОРКЕР РЕСАЙЗА
// ==========================================

type ResizeProcessor struct {
	s3     repository.MinioClientAPI
	repo   repository.AvatarRepository
	logger *slog.Logger
}

func NewResizeProcessor(s3 repository.MinioClientAPI, repo repository.AvatarRepository, log *slog.Logger) *ResizeProcessor {
	return &ResizeProcessor{
		s3:     s3,
		repo:   repo,
		logger: log,
	}
}

func (p *ResizeProcessor) ProcessResizeTask(ctx context.Context, data []byte) error {
	var task model.AvatarResizeTask
	if err := json.Unmarshal(data, &task); err != nil {
		return fmt.Errorf("failed to unmarshal JSON task: %w", err)
	}

	p.logger.InfoContext(ctx, "Processing resize for Avatar", "AvatarID", task.AvatarID)

	object, err := p.s3.GetObject(ctx, task.BucketName, task.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from minio: %w", err)
	}
	defer object.Close()

	// Ограничиваем чтение сверху (+1 байт для фиксации превышения лимита)
	limitedReader := io.LimitReader(object, MaxImageMemorySize+1)
	imgData, err := io.ReadAll(limitedReader)
	if err != nil {
		return fmt.Errorf("failed to read object data: %w", err)
	}

	// Защита от OOM: если файл больше лимита, прерываем обработку
	if int64(len(imgData)) > MaxImageMemorySize {
		return fmt.Errorf("downloaded image size exceeds maximum safe limit %d (OOM protection)", MaxImageMemorySize)
	}

	srcImg, imgType, err := image.Decode(bytes.NewReader(imgData))
	if err != nil {
		return fmt.Errorf("failed to decode image format: %w", err)
	}

	thumbnailsMap := make(map[string]string)
	ext := filepath.Ext(task.ObjectKey)

	for _, size := range task.Sizes {
		g := gift.New(gift.Resize(size, 0, gift.LanczosResampling))
		resizedImg := image.NewRGBA(g.Bounds(srcImg.Bounds()))
		g.Draw(resizedImg, srcImg)

		buf := new(bytes.Buffer)
		var encodeErr error

		switch imgType {
		case "png":
			encodeErr = png.Encode(buf, resizedImg)
		default:
			encodeErr = jpeg.Encode(buf, resizedImg, &jpeg.Options{Quality: 85})
		}

		if encodeErr != nil {
			return fmt.Errorf("failed to encode image size %d: %w", size, encodeErr)
		}

		thumbKey := fmt.Sprintf("minimals/%s_%d%s", task.AvatarID, size, ext)

		_, err = p.s3.PutObject(ctx, BucketName, thumbKey, buf, int64(buf.Len()), minio.PutObjectOptions{
			ContentType: "image/" + imgType,
		})
		if err != nil {
			return fmt.Errorf("failed to upload thumbnail %d to minio: %w", size, err)
		}

		thumbnailsMap[fmt.Sprintf("%d", size)] = fmt.Sprintf("/%s/%s", BucketName, thumbKey)
	}

	thumbnailsJSON, err := json.Marshal(thumbnailsMap)
	if err != nil {
		return fmt.Errorf("failed to marshal thumbnails map: %w", err)
	}

	err = p.repo.UpdateStatus(ctx, task.AvatarID, "success", thumbnailsJSON)
	if err != nil {
		return fmt.Errorf("failed to update avatar row in database: %w", err)
	}

	p.logger.InfoContext(ctx, "Successfully processed and saved thumbnails for Avatar", "AvatarID", task.AvatarID)

	// Явно освобождаем память и пинаем GC при интенсивной работе с графикой
	srcImg = nil
	runtime.GC()

	return nil
}

// ==========================================
// 2. ВОРКЕР УДАЛЕНИЯ (CLEANER)
// ==========================================

type AvatarDeleteWorker struct {
	s3     repository.MinioClientAPI
	logger *slog.Logger
}

func NewAvatarDeleteWorker(s3 repository.MinioClientAPI, log *slog.Logger) *AvatarDeleteWorker {
	return &AvatarDeleteWorker{
		s3:     s3,
		logger: log,
	}
}

func (w *AvatarDeleteWorker) ProcessDeleteTask(ctx context.Context, payload []byte) error {
	var task model.AvatarDeleteTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return fmt.Errorf("failed to unmarshal delete task: %w", err)
	}

	w.logger.InfoContext(ctx, "Worker starting physical cleanup for", "AvatarID", task.AvatarID, "TotalFiles", len(task.S3Keys))

	for _, key := range task.S3Keys {
		if key == "" {
			continue
		}
		err := w.s3.RemoveObject(ctx, BucketName, key, minio.RemoveObjectOptions{})
		if err != nil {
			w.logger.ErrorContext(ctx, "Worker failed to physically remove object from MinIO", "objectKey", key, "err", err)
			continue
		}
		w.logger.InfoContext(ctx, "Physically removed object from S3", "objectKey", key)
	}

	return nil
}

// ==========================================
// 3. ОСНОВНОЙ ПУЛ ДЛЯ ОДНОВРЕМЕННОГО ЗАПУСКА
// ==========================================

func main() {
	// Основной контекст жизненного цикла воркера
	mainCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Инициализируем провайдеры телеметрии
	var otelLogsShutdown func()
	logger.Log, otelLogsShutdown = logger.InitLoggerProvider(mainCtx)
	otelTracesShutdown := logger.InitTraceProvider(mainCtx)
	otelMetricsShutdown := logger.InitMeterProvider(mainCtx)

	config.InitWorker(mainCtx)

	repo := repository.NewPostgresAvatarRepository(config.ConnWorker.DB)
	s3Client := config.ConnWorker.MinioClient

	resizeProcessor := NewResizeProcessor(s3Client, repo, logger.Log)
	deleteWorker := NewAvatarDeleteWorker(s3Client, logger.Log)

	resizeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{config.CfgWorker.KafkaBrokers},
		Topic:          config.KafkaResizeTopic,
		GroupID:        KafkaResizeGroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,

		// prod-ready
		ReadBatchTimeout: 10 * time.Second, // Максимальное время ожидания ответа от брокера
		Dialer: &kafka.Dialer{
			Timeout:   5 * time.Second, // Таймаут первичного TCP-подключения к Kafka
			KeepAlive: 30 * time.Second,
		},
		// Стратегия балансировки при масштабировании подов воркера
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RoundRobinGroupBalancer{},
		},
	})
	deleteReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{config.CfgWorker.KafkaBrokers},
		Topic:          config.KafkaDeleteTopic,
		GroupID:        KafkaDeleteGroupID,
		MinBytes:       10e3,
		MaxBytes:       10e6,
		CommitInterval: 0,

		// prod-ready
		ReadBatchTimeout: 10 * time.Second, // Максимальное время ожидания ответа от брокера
		Dialer: &kafka.Dialer{
			Timeout:   5 * time.Second, // Таймаут первичного TCP-подключения к Kafka
			KeepAlive: 30 * time.Second,
		},
		// Стратегия балансировки при масштабировании подов воркера
		GroupBalancers: []kafka.GroupBalancer{
			kafka.RoundRobinGroupBalancer{},
		},
	})

	var wg sync.WaitGroup

	logger.Log.InfoContext(mainCtx, "Combined Avatar Worker Daemon started successfully!")

	// Рутина 1: Слушаем задачи на ресайз
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Log.InfoContext(mainCtx, "Subscribed to topic in kafka", "topic", config.KafkaResizeTopic)
		for {
			msg, err := resizeReader.FetchMessage(mainCtx)
			if err != nil {
				if mainCtx.Err() != nil {
					return // Корректный выход при остановке пода
				}
				logger.Log.ErrorContext(mainCtx, "Error fetching resize message", "err", err)
				time.Sleep(1 * time.Second) // Защита от бесконечного быстрого цикла при сбое сети
				continue
			}

			// Выделяем независимый контекст с таймаутом на одну задачу
			taskCtx, taskCancel := context.WithTimeout(context.Background(), 45*time.Second)

			func() {
				defer taskCancel()
				defer func() {
					if r := recover(); r != nil {
						logger.Log.ErrorContext(taskCtx, "PANIC RECOVERED in resize worker", "panic", r, "key", string(msg.Key))
					}
				}()

				if err := resizeProcessor.ProcessResizeTask(taskCtx, msg.Value); err != nil {
					logger.Log.ErrorContext(taskCtx, "Failed to resize avatar for key", "key", string(msg.Key), "err", err)
					return
				}

				if err := resizeReader.CommitMessages(taskCtx, msg); err != nil {
					logger.Log.ErrorContext(taskCtx, "Failed to commit resize message", "err", err)
				}
			}()
		}
	}()

	// Рутина 2: Слушаем задачи на удаление
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Log.InfoContext(mainCtx, "Subscribed to topic in kafka", "topic", config.KafkaDeleteTopic)
		for {
			msg, err := deleteReader.FetchMessage(mainCtx)
			if err != nil {
				if mainCtx.Err() != nil {
					return
				}
				logger.Log.ErrorContext(mainCtx, "Error fetching delete message", "err", err)
				time.Sleep(1 * time.Second)
				continue
			}

			taskCtx, taskCancel := context.WithTimeout(context.Background(), 30*time.Second)

			func() {
				defer taskCancel()
				defer func() {
					if r := recover(); r != nil {
						logger.Log.ErrorContext(taskCtx, "PANIC RECOVERED in delete worker", "panic", r, "key", string(msg.Key))
					}
				}()

				if err := deleteWorker.ProcessDeleteTask(taskCtx, msg.Value); err != nil {
					logger.Log.ErrorContext(taskCtx, "Failed to clear S3 layout for key", "key", string(msg.Key), "err", err)
					return
				}

				if err := deleteReader.CommitMessages(taskCtx, msg); err != nil {
					logger.Log.ErrorContext(taskCtx, "Failed to commit delete message", "err", err)
				}
			}()
		}
	}()

	// Ожидаем сигнал ОС
	<-mainCtx.Done()
	logger.Log.InfoContext(context.Background(), "Stopping worker consumers gracefully...")

	// Принудительно закрываем ридеры, прерывая зависшие FetchMessage
	_ = resizeReader.Close()
	_ = deleteReader.Close()

	// Ждем завершения активных обработчиков тасок
	wg.Wait()
	logger.Log.InfoContext(context.Background(), "All background processing loops stopped. Closing connections...")

	// Закрываем пулы инфраструктуры
	if config.ConnWorker.DB != nil {
		if err := config.ConnWorker.DB.Close(); err != nil {
			logger.Log.Error("Error closing DB connection", "error", err)
		}
	}

	// сброс телеметрии (Выполняем синхронно в самом конце)
	logger.Log.Info("Отправка оставшихся метрик, трейсов и логов в коллектор...")

	if otelMetricsShutdown != nil {
		otelMetricsShutdown()
	}
	if otelTracesShutdown != nil {
		otelTracesShutdown()
	}
	if otelLogsShutdown != nil {
		otelLogsShutdown()
	}

	// Пишем через стандартный slog, так как кастомный logger.Log к этому моменту уже закрыт
	slog.Info("Graceful shutdown успешно завершен.")
}
