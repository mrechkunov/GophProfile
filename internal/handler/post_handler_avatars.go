package handler

import (
	"encoding/json"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/logger"
	"gophprofile/internal/model"
	"gophprofile/internal/repository"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
)

const MaxFileSize = 10 * 1024 * 1024
const BucketName = "avatars"
const KafkaTopic = "avatar-resize-tasks"

type AvatarResponse struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

type SizeErrorResponse struct {
	Error   string `json:"error"`
	MaxSize int64  `json:"max_size"`
}

func PostUploadAvatarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing X-User-ID header"})
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, MaxFileSize)

	if err := r.ParseMultipartForm(MaxFileSize); err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			json.NewEncoder(w).Encode(SizeErrorResponse{Error: "File too large", MaxSize: MaxFileSize})
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid multipart form", Details: err.Error()})
		return
	}

	file, fileHeader, err := r.FormFile("image")
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing file field"})
		return
	}
	defer file.Close()

	if fileHeader.Size > MaxFileSize {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		json.NewEncoder(w).Encode(SizeErrorResponse{Error: "File too large", MaxSize: MaxFileSize})
		return
	}

	// Валидация Magic Bytes (Защита от подмены расширения)
	buff := make([]byte, 512)
	if _, err = file.Read(buff); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to read file header"})
		return
	}
	// Возвращаем указатель чтения в начало файла для последующей отправки в S3
	if _, err = file.Seek(0, 0); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Internal file seeking error"})
		return
	}

	realContentType := http.DetectContentType(buff)
	validTypes := map[string]bool{
		"image/jpeg": true,
		"image/png":  true,
		"image/webp": true,
	}

	if !validTypes[realContentType] {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "Invalid file format",
			Details: "Supported formats: jpeg, png, webp",
		})
		return
	}

	// Дополнительно проверяем расширение имени файла
	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
	if ext != ".jpeg" && ext != ".jpg" && ext != ".png" && ext != ".webp" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid file extension"})
		return
	}

	avatarID := uuid.New().String()
	objectKey := fmt.Sprintf("originals/%s%s", avatarID, ext)

	// Загрузка в MinIO
	_, err = config.ConnServer.MinioClient.PutObject(r.Context(), BucketName, objectKey, file, fileHeader.Size, minio.PutObjectOptions{
		ContentType: realContentType,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save file to storage"})
		logger.Log.Errorln("error while putting object into minio", err)
		return
	}

	// Механизм автоматического отката (Rollback) изменений в MinIO при ошибках ниже
	var success bool
	defer func() {
		if !success {
			// Если до конца функции success останется false — удаляем файл
			_ = config.ConnServer.MinioClient.RemoveObject(r.Context(), BucketName, objectKey, minio.RemoveObjectOptions{})
		}
	}()

	avatarURL := fmt.Sprintf("/%s/%s", BucketName, objectKey)

	// Запись метаданных в PostgreSQL
	avatar := model.Avatar{
		UUID:             avatarID,
		UserID:           userID,
		FileName:         fileHeader.Filename,
		MimeType:         realContentType,
		SizeBytes:        fileHeader.Size,
		S3Key:            objectKey,
		UploadStatus:     "uploading",
		ProcessingStatus: "processing",
	}

	storage := repository.NewPostgresAvatarRepository(config.ConnServer.DB)
	err = storage.Create(r.Context(), &avatar)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save avatar metadata"})
		logger.Log.Warnln("error while write new avatar in db", err)
		return
	}

	// Публикация задачи в Kafka
	task := model.AvatarResizeTask{
		AvatarID:   avatarID,
		UserID:     userID,
		BucketName: BucketName,
		ObjectKey:  objectKey,
		Sizes:      []int{100, 300},
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Internal event serialization error"})
		return
	}

	err = config.ConnServer.KafkaProducer.WriteMessages(r.Context(), kafka.Message{
		Topic: KafkaTopic,
		Key:   []byte(userID),
		Value: taskBytes,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to dispatch async task"})
		logger.Log.Errorln("error while send message in kafka(POST /api/v1/avatars)", err)
		return
	}

	// Все этапы выполнены без ошибок, сбрасываем триггер удаления файла
	success = true

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(AvatarResponse{
		ID:        avatarID,
		UserID:    userID,
		URL:       avatarURL,
		Status:    "processing",
		CreatedAt: time.Now().UTC(),
	})
}
