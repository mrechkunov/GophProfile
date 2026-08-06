package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"image"
	"image/jpeg" // Используется для jpeg.Encode
	"image/png"  // Используется для png.Encode
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"gophprofile/internal/config"
	"gophprofile/internal/logger"
	"gophprofile/internal/model"
	"gophprofile/internal/repository"

	"github.com/disintegration/gift"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
	"golang.org/x/image/webp" // Поддержка декодирования WebP
)

const (
	KafkaGroupID = "avatar-resize-worker-group"
	KafkaTopic   = "avatar-resize-tasks"
	BucketName   = "avatars"
)

func init() {
	// Регистрируем WebP декодер, так как в стандартной библиотеке его нет
	image.RegisterFormat("webp", "RIFF????WEBP", webp.Decode, webp.DecodeConfig)
}

func main() {
	config.InitWorker()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{config.CfgWorker.KafkaBrokers},
		Topic:    KafkaTopic,
		GroupID:  KafkaGroupID,
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	defer reader.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Log.Infoln("Avatar resize worker started...")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			fmt.Println("kafka adress:", config.CfgWorker.KafkaBrokers)
			logger.Log.Errorln("Error while fetching message from Kafka:", err)
			continue
		}

		if err := processResizeTask(ctx, msg.Value); err != nil {
			logger.Log.Errorf("Failed to process task for key %s: %v", string(msg.Key), err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			logger.Log.Errorln("Failed to commit message in Kafka:", err)
		}
	}

	logger.Log.Infoln("Worker gracefully stopped.")
}

func processResizeTask(ctx context.Context, data []byte) error {
	var task model.AvatarResizeTask
	if err := json.Unmarshal(data, &task); err != nil {
		return fmt.Errorf("failed to unmarshal JSON task: %w", err)
	}

	logger.Log.Infof("Processing resize for AvatarID: %s", task.AvatarID)

	// Скачиваем оригинальный файл из MinIO
	object, err := config.ConnWorker.MinioClient.GetObject(ctx, task.BucketName, task.ObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("failed to get object from minio: %w", err)
	}
	defer object.Close()

	imgData, err := io.ReadAll(object)
	if err != nil {
		return fmt.Errorf("failed to read object data: %w", err)
	}

	// Декодируем байты в картинку
	srcImg, imgType, err := image.Decode(strings.NewReader(string(imgData)))
	if err != nil {
		return fmt.Errorf("failed to decode image format: %w", err)
	}

	thumbnailsMap := make(map[string]string)
	ext := filepath.Ext(task.ObjectKey)

	// Перебираем размеры (100 и 300)
	for _, size := range task.Sizes {
		// Делаем ресайз через GIFT
		g := gift.New(gift.Resize(size, 0, gift.LanczosResampling))

		// Создаем пустой холст нужного размера под результат
		resizedImg := image.NewRGBA(g.Bounds(srcImg.Bounds()))
		g.Draw(resizedImg, srcImg)

		// Безопасное кодирование в буфер памяти (выделяет мало памяти, так как аватарки крошечные)
		buf := new(bytes.Buffer)
		var encodeErr error

		// кодируем через пакеты jpeg и png напрямую
		switch imgType {
		case "png":
			encodeErr = png.Encode(buf, resizedImg)
		default: // jpeg / jpg / webp
			encodeErr = jpeg.Encode(buf, resizedImg, &jpeg.Options{Quality: 85}) // Задаем оптимальное качество
		}

		if encodeErr != nil {
			return fmt.Errorf("failed to encode image size %d: %w", size, encodeErr)
		}

		// Новый ключ для уменьшенной копии в папку "minimals"
		thumbKey := fmt.Sprintf("minimals/%s_%d%s", task.AvatarID, size, ext)

		// Загружаем миниатюру в MinIO, передавая точный размер буфера `int64(buf.Len())`
		_, err = config.ConnWorker.MinioClient.PutObject(ctx, BucketName, thumbKey, buf, int64(buf.Len()), minio.PutObjectOptions{
			ContentType: "image/" + imgType,
		})
		if err != nil {
			return fmt.Errorf("failed to upload thumbnail %d to minio: %w", size, err)
		}

		thumbnailsMap[fmt.Sprintf("%d", size)] = fmt.Sprintf("/%s/%s", BucketName, thumbKey)
	}

	// Маршалим мапу для записи в JSONB поле базы данных
	thumbnailsJSON, err := json.Marshal(thumbnailsMap)
	if err != nil {
		return fmt.Errorf("failed to marshal thumbnails map: %w", err)
	}
	// Обновляем строку в PostgreSQL
	storage := repository.NewPostgresAvatarRepository(config.ConnWorker.DB)
	err = storage.UpdateStatus(ctx, task.AvatarID, "success", thumbnailsJSON)
	if err != nil {
		return fmt.Errorf("failed to update avatar row in database: %w", err)
	}

	logger.Log.Infof("Successfully processed and saved thumbnails for AvatarID: %s", task.AvatarID)
	return nil
}
