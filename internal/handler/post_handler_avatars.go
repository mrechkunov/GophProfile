package handler

// import (
// 	"context"
// 	"encoding/json"
// 	"fmt"
// 	"gophprofile/internal/model"
// 	"net/http"
// 	"path/filepath"
// 	"strings"
// 	"time"

// 	"github.com/google/uuid"
// )

// const MaxFileSize = 10 * 1024 * 1024
// const BucketName = "avatars"
// const KafkaTopic = "avatar-resize-tasks"

// // AppDependecies содержит клиенты внешних сервисов
// type AppDependecies struct {
// 	MinioClient *minio.Client
// 	KafkaProd   *kafka.Producer
// }

// type AvatarResponse struct {
// 	ID        string    `json:"id"`
// 	UserID    string    `json:"user_id"`
// 	URL       string    `json:"url"`
// 	Status    string    `json:"status"`
// 	CreatedAt time.Time `json:"created_at"`
// }

// type ErrorResponse struct {
// 	Error   string `json:"error"`
// 	Details string `json:"details,omitempty"`
// }

// type SizeErrorResponse struct {
// 	Error   string `json:"error"`
// 	MaxSize int64  `json:"max_size"`
// }

// // UploadAvatarHandler обрабатывает загрузку, сохраняет в MinIO и отправляет задачу в Kafka
// func (app *AppDependecies) UploadAvatarHandler(w http.ResponseWriter, r *http.Request) {
// 	w.Header().Set("Content-Type", "application/json")

// 	if r.Method != http.MethodPost {
// 		w.WriteHeader(http.StatusMethodNotAllowed)
// 		return
// 	}

// 	userID := r.Header.Get("X-User-ID")
// 	if userID == "" {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing X-User-ID header"})
// 		return
// 	}

// 	r.Body = http.MaxBytesReader(w, r.Body, MaxFileSize)

// 	if err := r.ParseMultipartForm(MaxFileSize); err != nil {
// 		w.WriteHeader(http.StatusRequestEntityTooLarge)
// 		json.NewEncoder(w).Encode(SizeErrorResponse{Error: "File too large", MaxSize: MaxFileSize})
// 		return
// 	}

// 	file, fileHeader, err := r.FormFile("file")
// 	if err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing file field"})
// 		return
// 	}
// 	defer file.Close()

// 	if fileHeader.Size > MaxFileSize {
// 		w.WriteHeader(http.StatusRequestEntityTooLarge)
// 		json.NewEncoder(w).Encode(SizeErrorResponse{Error: "File too large", MaxSize: MaxFileSize})
// 		return
// 	}

// 	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
// 	if ext != ".jpeg" && ext != ".jpg" && ext != ".png" && ext != ".webp" {
// 		w.WriteHeader(http.StatusBadRequest)
// 		json.NewEncoder(w).Encode(ErrorResponse{
// 			Error:   "Invalid file format",
// 			Details: "Supported formats: jpeg, png, webp",
// 		})
// 		return
// 	}

// 	// Генерация уникального ID для аватара и имени объекта в S3
// 	avatarID := uuid.New().String()
// 	objectKey := fmt.Sprintf("originals/%s%s", avatarID, ext)

// 	// 1. Загрузка оригинального файла в MinIO
// 	_, err = app.MinioClient.PutObject(context.Background(), BucketName, objectKey, file, fileHeader.Size, minio.PutObjectOptions{
// 		ContentType: fileHeader.Header.Get("Content-Type"),
// 	})
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save file to storage"})
// 		return
// 	}

// 	// URL, по которому файл будет доступен (формируется логика раздачи через MinIO или CDN)
// 	avatarURL := fmt.Sprintf("/%s/%s", BucketName, objectKey)

// 	// 2. Формирование сообщения для Kafka
// 	task := model.AvatarResizeTask{
// 		AvatarID:   avatarID,
// 		UserID:     userID,
// 		BucketName: BucketName,
// 		ObjectKey:  objectKey,
// 		Sizes:      []int{100, 300},
// 	}

// 	taskBytes, err := json.Marshal(task)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		json.NewEncoder(w).Encode(ErrorResponse{Error: "Internal event serialization error"})
// 		return
// 	}

// 	// 3. Отправка сообщения в Kafka асинхронно
// 	err = app.KafkaProd.Produce(&kafka.Message{
// 		TopicString: stringPtr(KafkaTopic),
// 		Key:         []byte(userID), // Партиционирование по UserID гарантирует последовательность
// 		Value:       taskBytes,
// 	}, nil)

// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to dispatch async task"})
// 		return
// 	}

// 	// 4. Успешный ответ 201 Created
// 	w.WriteHeader(http.StatusCreated)
// 	json.NewEncoder(w).Encode(AvatarResponse{
// 		ID:        avatarID,
// 		UserID:    userID,
// 		URL:       avatarURL,
// 		Status:    "processing",
// 		CreatedAt: time.Now().UTC(),
// 	})
// }

// func stringPtr(s string) *string {
// 	return &s
// }
