package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/model"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	_ "golang.org/x/image/webp"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/segmentio/kafka-go"
)

// POST /api/v1/avatars
func (h *AvatarHandler) PostUploadAvatarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Создаем корневой спан для всего HTTP запроса
	ctx, span := tracer.Start(r.Context(), "PostUploadAvatarHandler",
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	h.logger.InfoContext(ctx, "Processing AvatarUpload request")

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "missing_user_id"),
		))
		span.SetStatus(codes.Error, "Missing X-User-ID header")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing X-User-ID header"})
		return
	}
	span.SetAttributes(attribute.String("user.id", userID))

	r.Body = http.MaxBytesReader(w, r.Body, MaxFileSize)
	defer r.Body.Close()

	if err := r.ParseMultipartForm(MaxFileSize); err != nil {
		reason := "invalid_multipart"
		if strings.Contains(err.Error(), "request body too large") {
			reason = "body_too_large"
			h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("status", "error"),
				attribute.String("reason", reason),
			))
			span.RecordError(err)
			span.SetStatus(codes.Error, "Request body too large")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			json.NewEncoder(w).Encode(SizeErrorResponse{Error: "File too large", MaxSize: MaxFileSize})
			return
		}
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", reason),
		))
		span.RecordError(err)
		span.SetStatus(codes.Error, "Invalid multipart form")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid multipart form", Details: err.Error()})
		return
	}

	file, fileHeader, err := r.FormFile("image")
	if err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "missing_file_field"),
		))
		span.SetStatus(codes.Error, "Missing file field")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing file field"})
		return
	}
	defer file.Close()

	if fileHeader.Size > MaxFileSize {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "file_too_large"),
		))
		span.SetStatus(codes.Error, "File size limit exceeded")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		json.NewEncoder(w).Encode(SizeErrorResponse{Error: "File too large", MaxSize: MaxFileSize})
		return
	}

	buff := make([]byte, 512)
	if _, err = file.Read(buff); err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "read_header_failed"),
		))
		span.RecordError(err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to read file header"})
		return
	}

	if _, err = file.Seek(0, io.SeekStart); err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "seek_failed"),
		))
		span.RecordError(err)
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
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "invalid_mime_type"),
		))
		span.SetStatus(codes.Error, "Unsupported MIME type: "+realContentType)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "Invalid file format",
			Details: "Supported formats: jpeg, png, webp",
		})
		return
	}

	ext := strings.ToLower(filepath.Ext(fileHeader.Filename))
	if ext != ".jpeg" && ext != ".jpg" && ext != ".png" && ext != ".webp" {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "invalid_extension"),
		))
		span.SetStatus(codes.Error, "Unsupported file extension")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid file extension"})
		return
	}

	imgConfig, _, err := image.DecodeConfig(file)
	if err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "decode_config_failed"),
		))
		span.RecordError(err)
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid image data", Details: "Failed to parse dimensions"})
		return
	}

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "seek_post_decode_failed"),
		))
		span.RecordError(err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Internal file seeking error"})
		return
	}

	avatarID := uuid.New().String()
	objectKey := fmt.Sprintf("originals/%s%s", avatarID, ext)

	span.SetAttributes(
		attribute.String("avatar.id", avatarID),
		attribute.String("avatar.s3_key", objectKey),
	)

	var s3Uploaded bool
	var dbCreated bool

	defer func() {
		if !s3Uploaded && !dbCreated {
			return
		}
		cleanupCtx := context.Background()
		if s3Uploaded {
			_ = h.s3.RemoveObject(cleanupCtx, BucketName, objectKey, minio.RemoveObjectOptions{})
		}
		if dbCreated {
			_, _ = h.repo.SoftDelete(cleanupCtx, avatarID)
		}
	}()

	// Вложенный Спан для Minio
	err = func() error {
		_, s3Span := tracer.Start(ctx, "Minio:PutObject", trace.WithSpanKind(trace.SpanKindClient))
		defer s3Span.End()

		_, putErr := h.s3.PutObject(ctx, BucketName, objectKey, file, fileHeader.Size, minio.PutObjectOptions{
			ContentType: realContentType,
		})
		if putErr != nil {
			s3Span.RecordError(putErr)
			s3Span.SetStatus(codes.Error, "S3 Upload Failed")
			return putErr
		}
		return nil
	}()

	if err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "s3_upload_failed"),
		))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save file to storage"})
		h.logger.ErrorContext(ctx, "error while putting object into minio", "err", err)
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
		Width:            imgConfig.Width,
		Height:           imgConfig.Height,
	}

	// Вложенный Спан для Базы Данных
	err = func() error {
		_, dbSpan := tracer.Start(ctx, "Database:CreateAvatar", trace.WithSpanKind(trace.SpanKindClient))
		defer dbSpan.End()

		return h.repo.Create(ctx, &avatar)
	}()

	if err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "db_create_failed"),
		))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to save avatar metadata"})
		h.logger.ErrorContext(ctx, "error while write new avatar in db", "err", err)
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
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "json_marshal_failed"),
		))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "internal event serialization error"})
		return
	}

	// Вложенный Спан для Кафки
	err = func() error {
		_, kafkaSpan := tracer.Start(ctx, "Kafka:WriteMessage", trace.WithSpanKind(trace.SpanKindProducer))
		defer kafkaSpan.End()

		return h.kafka.WriteMessages(ctx, kafka.Message{
			Topic: config.KafkaResizeTopic,
			Key:   []byte(userID),
			Value: taskBytes,
		})
	}()

	if err != nil {
		h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "kafka_publish_failed"),
		))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "failed to dispatch async task"})
		h.logger.ErrorContext(ctx, "error while send message in kafka (POST /api/v1/avatars)", "err", err)
		return
	}

	// Успешный исход — отменяем очистку дефером
	s3Uploaded = false
	dbCreated = false

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(AvatarResponse{
		ID:        avatarID,
		UserID:    userID,
		URL:       avatarURL,
		Status:    "processing",
		CreatedAt: time.Now().UTC()})
	h.logger.InfoContext(ctx, "UploadAvatar request processing completed")
	// Записываем финальные метрики успеха
	h.metrics.UploadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
	h.metrics.FileSizeHist.Record(ctx, float64(fileHeader.Size), metric.WithAttributes(attribute.String("content_type", realContentType)))
	span.SetStatus(codes.Ok, "Avatar uploaded successfully")
}
