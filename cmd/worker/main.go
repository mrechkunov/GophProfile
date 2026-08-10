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
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

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
	s3   repository.MinioClientAPI
	repo repository.AvatarRepository
}

func NewResizeProcessor(s3 repository.MinioClientAPI, repo repository.AvatarRepository) *ResizeProcessor {
	return &ResizeProcessor{s3: s3, repo: repo}
}

func (p *ResizeProcessor) ProcessResizeTask(ctx context.Context, data []byte) error {
	var task model.AvatarResizeTask
	if err := json.Unmarshal(data, &task); err != nil {
		return fmt.Errorf("failed to unmarshal JSON task: %w", err)
	}

	logger.Log.Infof("Processing resize for AvatarID: %s", task.AvatarID)

	object, err := p.s3.GetObject(ctx, task.BucketName, task.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from minio: %w", err)
	}
	defer object.Close()

	imgData, err := io.ReadAll(object)
	if err != nil {
		return fmt.Errorf("failed to read object data: %w", err)
	}

	srcImg, imgType, err := image.Decode(strings.NewReader(string(imgData)))
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

	logger.Log.Infof("Successfully processed and saved thumbnails for AvatarID: %s", task.AvatarID)
	return nil
}

// ==========================================
// 2. ВОРКЕР УДАЛЕНИЯ (CLEANER)
// ==========================================

type AvatarDeleteWorker struct {
	s3 repository.MinioClientAPI
}

func NewAvatarDeleteWorker(s3 repository.MinioClientAPI) *AvatarDeleteWorker {
	return &AvatarDeleteWorker{s3: s3}
}

func (w *AvatarDeleteWorker) ProcessDeleteTask(ctx context.Context, payload []byte) error {
	var task model.AvatarDeleteTask
	if err := json.Unmarshal(payload, &task); err != nil {
		return fmt.Errorf("failed to unmarshal delete task: %w", err)
	}

	logger.Log.Infof("Worker starting physical cleanup for AvatarID: %s. Total files: %d", task.AvatarID, len(task.S3Keys))

	for _, key := range task.S3Keys {
		if key == "" {
			continue
		}
		err := w.s3.RemoveObject(ctx, BucketName, key, minio.RemoveObjectOptions{})
		if err != nil {
			logger.Log.Errorf("Worker failed to physically remove object %s from MinIO: %v", key, err)
			continue
		}
		logger.Log.Infof("Physically removed object from S3: %s", key)
	}

	return nil
}

// ==========================================
// 3. ОСНОВНОЙ ПУЛ ДЛЯ ОДНОВРЕМЕННОГО ЗАПУСКА
// ==========================================

func main() {
	config.InitWorker()

	// Инициализируем общие для обоих процессов зависимости (БД на pgx/v5 и MinIO)
	repo := repository.NewPostgresAvatarRepository(config.ConnWorker.DB)
	s3Client := config.ConnWorker.MinioClient

	// Создаем экземпляры наших процессоров логики
	resizeProcessor := NewResizeProcessor(s3Client, repo)
	deleteWorker := NewAvatarDeleteWorker(s3Client)

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

	logger.Log.Infoln("Combined Avatar Worker Daemon started successfully...")

	// Рутина 1: Слушаем задачи на ресайз
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Log.Infoln("Subscribed to topic:", config.KafkaResizeTopic)
		for {
			msg, err := resizeReader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				logger.Log.Errorln("Error fetching resize message:", err)
				continue
			}

			if err := resizeProcessor.ProcessResizeTask(ctx, msg.Value); err != nil {
				logger.Log.Errorf("Failed to resize avatar for key %s: %v", string(msg.Key), err)
				continue
			}

			if err := resizeReader.CommitMessages(ctx, msg); err != nil {
				logger.Log.Errorln("Failed to commit resize message:", err)
			}
		}
	}()

	// Рутина 2: Слушаем задачи на очистку хранилища (Soft Delete)
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Log.Infoln("Subscribed to topic:", config.KafkaDeleteTopic)
		for {
			msg, err := deleteReader.FetchMessage(ctx)
			if err != nil {
				if ctx.Err() != nil {
					break
				}
				logger.Log.Errorln("Error fetching delete message:", err)
				continue
			}

			if err := deleteWorker.ProcessDeleteTask(ctx, msg.Value); err != nil {
				logger.Log.Errorf("Failed to clear S3 layout for key %s: %v", string(msg.Key), err)
				continue
			}

			if err := deleteReader.CommitMessages(ctx, msg); err != nil {
				logger.Log.Errorln("Failed to commit delete message:", err)
			}
		}
	}()

	// Ожидаем завершения горутин при системном сигнале SIGTERM
	<-ctx.Done()
	logger.Log.Infoln("Stopping worker consumers gracefully...")
	wg.Wait()
	logger.Log.Infoln("All background processes successfully stopped.")
}
