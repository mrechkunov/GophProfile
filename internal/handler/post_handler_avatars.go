package handler

import (
	"encoding/json"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/logger"
	"gophprofile/internal/model"
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

// PostUploadAvatarHandler обрабатывает загрузку, сохраняет в MinIO и отправляет задачу в Kafka
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

	// Ограничиваем максимальный размер тела запроса сверху
	r.Body = http.MaxBytesReader(w, r.Body, MaxFileSize)

	if err := r.ParseMultipartForm(MaxFileSize); err != nil {
		// ИСПРАВЛЕНО: проверяем, была ли ошибка вызвана именно превышением размера
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

	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
	if ext != ".jpeg" && ext != ".jpg" && ext != ".png" && ext != ".webp" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "Invalid file format",
			Details: "Supported formats: jpeg, png, webp",
		})
		return
	}

	avatarID := uuid.New().String()
	objectKey := fmt.Sprintf("originals/%s%s", avatarID, ext)

	// ИСПРАВЛЕНО: Передаем r.Context() вместо context.Background() для своевременной отмены операции
	_, err = config.Conn.MinioClient.PutObject(r.Context(), BucketName, objectKey, file, fileHeader.Size, minio.PutObjectOptions{
		ContentType: fileHeader.Header.Get("Content-Type"),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save file to storage"})
		logger.Log.Errorln("error while putting object into minio", err)
		return
	}

	avatarURL := fmt.Sprintf("/%s/%s", BucketName, objectKey)

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

	// ИСПРАВЛЕНО: Использование r.Context() вместо context.Background()
	err = config.Conn.KafkaProducer.WriteMessages(r.Context(), kafka.Message{
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

	// TODO: записать данные в БД со статусом "processing"

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(AvatarResponse{
		ID:        avatarID,
		UserID:    userID,
		URL:       avatarURL,
		Status:    "processing",
		CreatedAt: time.Now().UTC(),
	})
}
