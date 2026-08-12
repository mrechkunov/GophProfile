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
)

func init() {
	// Регистрируем WebP декодер для ресайза
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

func NewResizeProcessor(s3 repository.MinioClientAPI, repo repository.AvatarRepository, logger *slog.Logger) *ResizeProcessor {
	return &ResizeProcessor{s3: s3, repo: repo, logger: logger}
}

func (p *ResizeProcessor) ProcessResizeTask(ctx context.Context, data []byte) error {
	var task model.AvatarResizeTask
	if err := json.Unmarshal(data, &task); err != nil {
		return fmt.Errorf("failed to unmarshal JSON task: %w", err)
	}

	p.logger.InfoContext(ctx, "Processing resize for AvatarID", "AvatarID", task.AvatarID)

	object, err := p.s3.GetObject(ctx, task.BucketName, task.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from minio: %w", err)
	}
	defer object.Close()

	imgData, err := io.ReadAll(object)
	if err != nil {
		return fmt.Errorf("failed to read object data: %w", err)
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

	p.logger.InfoContext(ctx, "Successfully processed and saved thumbnails for AvatarID", "AvatarID", task.AvatarID)
	return nil
}

// ==========================================
// 2. ВОРКЕР УДАЛЕНИЯ (CLEANER)
// ==========================================

type AvatarDeleteWorker struct {
	s3     repository.MinioClientAPI
	logger *slog.Logger
}

func NewAvatarDeleteWorker(s3 repository.MinioClientAPI, logger *slog.Logger) *AvatarDeleteWorker {
	return &AvatarDeleteWorker{
		s3:     s3,
		logger: logger,
	}
}

func (w *AvatarDeleteWorker) ProcessDeleteTask(ctx context.Context, payload []byte) error {
	var task model.AvatarDeleteTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return fmt.Errorf("failed to unmarshal delete task: %w", err)
	}

	w.logger.InfoContext(ctx, "Worker starting physical cleanup for ", "AvatarID:", task.AvatarID, "Total files:", len(task.S3Keys))

	for _, key := range task.S3Keys {
		if key == "" {
			continue
		}
		err := w.s3.RemoveObject(ctx, BucketName, key, minio.RemoveObjectOptions{})
		if err != nil {
			w.logger.ErrorContext(ctx, "Worker failed to physically remove object from MinIO:", "object key", key, "err", err)
			continue
		}
		w.logger.InfoContext(ctx, "Physically removed object from S3:", "object key", key)
	}

	return nil
}

// ==========================================
// 3. ОСНОВНОЙ ПУЛ ДЛЯ ОДНОВРЕМЕННОГО ЗАПУСКА
// ==========================================

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	config.InitWorker(ctx)

	// Инициализируем общие для обоих процессов зависимости (БД на pgx/v5 и MinIO)
	repo := repository.NewPostgresAvatarRepository(config.ConnWorker.DB)
	s3Client := config.ConnWorker.MinioClient

	// Создаем экземпляры наших процессоров логики
	resizeProcessor := NewResizeProcessor(s3Client, repo, logger.Log)
	deleteWorker := NewAvatarDeleteWorker(s3Client, logger.Log)

	// Настраиваем Kafka ридеров для каждого топика
	resizeReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{config.CfgWorker.KafkaBrokers},
		Topic:    config.KafkaResizeTopic,
		GroupID:  KafkaResizeGroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer resizeReader.Close()

	deleteReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{config.CfgWorker.KafkaBrokers},
		Topic:    config.KafkaDeleteTopic,
		GroupID:  KafkaDeleteGroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer deleteReader.Close()

	// Настраиваем Graceful Shutdown через контекст
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	logger.Log.InfoContext(ctx, "Combined Avatar Worker Daemon started successfully...")

	// Рутина 1: Слушаем задачи на ресайз
	wg.Go(func() {
		logger.Log.InfoContext(ctx, "Subscribed to topic:", "", config.KafkaResizeTopic)
		for {
			msg, err := resizeReader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				logger.Log.ErrorContext(ctx, "Error fetching resize message:", "err", err)
				continue
			}

			if err := resizeProcessor.ProcessResizeTask(ctx, msg.Value); err != nil {
				logger.Log.ErrorContext(ctx, "Failed to resize avatar for key:", "key", string(msg.Key), "err", err)
				continue
			}

			if err := resizeReader.CommitMessages(ctx, msg); err != nil {
				logger.Log.ErrorContext(ctx, "Failed to commit resize message:", "err", err)
			}
		}
	})

	// Рутина 2: Слушаем задачи на очистку хранилища (Soft Delete)
	wg.Go(func() {
		logger.Log.InfoContext(ctx, "Subscribed to topic:", "", config.KafkaDeleteTopic)
		for {
			msg, err := deleteReader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				logger.Log.ErrorContext(ctx, "Error fetching delete message:", "err", err)
				continue
			}

			if err := deleteWorker.ProcessDeleteTask(ctx, msg.Value); err != nil {
				logger.Log.ErrorContext(ctx, "Failed to clear S3 layout for key:", "key", string(msg.Key), "err", err)
				continue
			}

			if err := deleteReader.CommitMessages(ctx, msg); err != nil {
				logger.Log.ErrorContext(ctx, "Failed to commit delete message:", "err", err)
			}
		}
	})

	// Ожидаем завершения горутин при системном сигнале SIGTERM
	<-ctx.Done()
	logger.Log.InfoContext(ctx, "Stopping worker consumers gracefully...")
	wg.Wait()
	logger.Log.InfoContext(ctx, "All background processes successfully stopped.")
}
