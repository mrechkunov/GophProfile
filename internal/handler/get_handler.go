package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/minio/minio-go/v7"
)

// GET /api/v1/avatars/{avatar_id}
func (h *AvatarHandler) GetAvatarHandler(w http.ResponseWriter, r *http.Request) {
	avatarID := chi.URLParam(r, "avatar_id")
	if avatarID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing avatar_id parameter"})
		return
	}

	size := r.URL.Query().Get("size")
	if size == "" {
		size = "original"
	}
	format := r.URL.Query().Get("format")

	if size != "100x100" && size != "300x300" && size != "original" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid size parameter"})
		return
	}

	if format != "" && format != "jpeg" && format != "png" && format != "webp" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid format parameter"})
		return
	}

	avatar, err := h.repo.GetByID(r.Context(), avatarID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar not found"})
		return
	}

	var objectKey string
	switch size {
	case "100x100":
		objectKey = avatar.Thumbnail_S3_Keys.Small
	case "300x300":
		objectKey = avatar.Thumbnail_S3_Keys.Medium
	default:
		objectKey = avatar.S3Key
	}

	if objectKey == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Requested size not processed yet"})
		return
	}

	// Запрашиваем нативный объект
	object, err := h.s3.GetObject(r.Context(), BucketName, objectKey, minio.GetObjectOptions{})
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to retrieve image from storage"})
		return
	}
	defer object.Close()

	// Снова используем Stat() нативного объекта
	objInfo, err := object.Stat()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar file not found in storage"})
		return
	}

	contentType := objInfo.ContentType
	if contentType == "" {
		contentType = avatar.MimeType
	}
	if format != "" {
		contentType = "image/" + format
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "max-age=86400")
	if objInfo.ETag != "" {
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, strings.Trim(objInfo.ETag, `"`)))
	}

	w.WriteHeader(http.StatusOK)

	// Копируем напрямую из object
	_, err = io.Copy(w, object)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "error while coping from s3 object to responce", "err", err)
	}
}

// GET /api/v1/avatars/{avatar_id}/metadata
func (h *AvatarHandler) GetAvatarMetadataHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	avatarID := chi.URLParam(r, "avatar_id")
	if avatarID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing avatar_id parameter"})
		return
	}

	avatar, err := h.repo.GetByID(r.Context(), avatarID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar not found"})
		return
	}

	var thumbnails []ThumbnailInfo
	if avatar.Thumbnail_S3_Keys.Small != "" {
		thumbnails = append(thumbnails, ThumbnailInfo{
			Size: "100x100",
			URL:  fmt.Sprintf("/%s/%s", BucketName, avatar.Thumbnail_S3_Keys.Small),
		})
	}
	if avatar.Thumbnail_S3_Keys.Medium != "" {
		thumbnails = append(thumbnails, ThumbnailInfo{
			Size: "300x300",
			URL:  fmt.Sprintf("/%s/%s", BucketName, avatar.Thumbnail_S3_Keys.Medium),
		})
	}

	dims := Dimensions{
		Width:  avatar.Width,
		Height: avatar.Height,
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(AvatarMetadataResponse{
		ID:         avatar.UUID,
		UserID:     avatar.UserID,
		FileName:   avatar.FileName,
		MimeType:   avatar.MimeType,
		Size:       avatar.SizeBytes,
		Dimensions: dims,
		Thumbnails: thumbnails,
		CreatedAt:  avatar.CreatedAt,
		UpdatedAt:  avatar.UpdatedAt,
	})
}

// GET /api/v1/users/{user_id}/avatar
func (h *AvatarHandler) GetUserAvatarHandler(w http.ResponseWriter, r *http.Request) {

	userID := chi.URLParam(r, "user_id")
	if userID == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing user_id parameter"})
		return
	}

	size := r.URL.Query().Get("size")
	if size == "" {
		size = "original"
	}
	format := r.URL.Query().Get("format")

	if size != "100x100" && size != "300x300" && size != "original" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid size parameter"})
		return
	}

	if format != "" && format != "jpeg" && format != "png" && format != "webp" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid format parameter"})
		return
	}

	// Читаем последнюю активную аватарку из БД
	avatar, err := h.repo.GetByUserID(r.Context(), userID)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar for this user not found"})
		return
	}

	var objectKey string
	switch size {
	case "100x100":
		objectKey = avatar.Thumbnail_S3_Keys.Small
	case "300x300":
		objectKey = avatar.Thumbnail_S3_Keys.Medium
	default:
		objectKey = avatar.S3Key
	}

	if objectKey == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Requested size not processed yet"})
		return
	}

	// Запрашиваем нативный объект из MinIO
	object, err := h.s3.GetObject(r.Context(), BucketName, objectKey, minio.GetObjectOptions{})
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to retrieve image from storage"})
		return
	}
	defer object.Close() // Закрываем нативный объект

	// Получаем метаданные напрямую через нативный метод Stat()
	objInfo, err := object.Stat()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar file not found in storage"})
		return
	}

	contentType := objInfo.ContentType
	if contentType == "" {
		contentType = avatar.MimeType
	}
	if format != "" {
		contentType = "image/" + format
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "max-age=86400")
	if objInfo.ETag != "" {
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, strings.Trim(objInfo.ETag, `"`)))
	}

	w.WriteHeader(http.StatusOK)

	// Пишем байты из нативного *minio.Object в HTTP-ответ
	_, err = io.Copy(w, object)
	if err != nil {
		h.logger.ErrorContext(r.Context(), "error while coping from s3 object to responce", "err", err)
	}
}
