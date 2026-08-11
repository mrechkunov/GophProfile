package handler

import (
	"encoding/json"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/model"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
)

// POST /api/v1/avatars
func (h *AvatarHandler) PostUploadAvatarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	h.logger.InfoContext(r.Context(), "POST incoming")
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
	defer r.Body.Close()

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

	buff := make([]byte, 512)
	if _, err = file.Read(buff); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to read file header"})
		return
	}

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

	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
	if ext != ".jpeg" && ext != ".jpg" && ext != ".png" && ext != ".webp" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid file extension"})
		return
	}

	avatarID := uuid.New().String()
	objectKey := fmt.Sprintf("originals/%s%s", avatarID, ext)

	var s3Uploaded bool
	var dbCreated bool

	defer func() {
		if !s3Uploaded && !dbCreated {
			return
		}
		// Запускаем очистку ресурсов при сбое
		if s3Uploaded {
			_ = h.s3.RemoveObject(r.Context(), BucketName, objectKey, minio.RemoveObjectOptions{})
		}
		if dbCreated {
			// Вызываем SoftDelete вместо жесткого удаления строки
			_, _ = h.repo.SoftDelete(r.Context(), avatarID)
		}
	}()

	_, err = h.s3.PutObject(r.Context(), BucketName, objectKey, file, fileHeader.Size, minio.PutObjectOptions{
		ContentType: realContentType,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save file to storage"})
		h.logger.ErrorContext(r.Context(), "error while putting object into minio")
		return
	}
	s3Uploaded = true

	avatarURL := fmt.Sprintf("/%s/%s", BucketName, objectKey)

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

	err = h.repo.Create(r.Context(), &avatar)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save avatar metadata"})
		h.logger.WarnContext(r.Context(), "error while write new avatar in db")
		return
	}
	dbCreated = true

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
		json.NewEncoder(w).Encode(ErrorResponse{Error: "internal event serialization error"})
		return
	}

	err = h.kafka.WriteMessages(r.Context(), kafka.Message{
		Topic: config.KafkaResizeTopic,
		Key:   []byte(userID),
		Value: taskBytes,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to dispatch async task"})
		h.logger.ErrorContext(r.Context(), "error while send message in kafka (POST /api/v1/avatars)")
		return
	}

	s3Uploaded = false
	dbCreated = false

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(AvatarResponse{
		ID:        avatarID,
		UserID:    userID,
		URL:       avatarURL,
		Status:    "processing",
		CreatedAt: time.Now().UTC(),
	})
}
