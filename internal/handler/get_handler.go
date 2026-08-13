package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"gophprofile/internal/model"

	"github.com/go-chi/chi/v5"
	"github.com/minio/minio-go/v7"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// GET /api/v1/avatars/{avatar_id}
func (h *AvatarHandler) GetAvatarHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "GetAvatarHandler", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	avatarID := chi.URLParam(r, "avatar_id")
	if avatarID == "" {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "missing_avatar_id")))
		span.SetStatus(codes.Error, "Missing avatar_id parameter")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing avatar_id parameter"})
		return
	}
	span.SetAttributes(attribute.String("avatar.id", avatarID))

	size := r.URL.Query().Get("size")
	if size == "" {
		size = "original"
	}
	format := r.URL.Query().Get("format")

	span.SetAttributes(attribute.String("requested.size", size), attribute.String("requested.format", format))

	if size != "100x100" && size != "300x300" && size != "original" {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "invalid_size")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid size parameter"})
		return
	}

	if format != "" && format != "jpeg" && format != "png" && format != "webp" {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "invalid_format")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid format parameter"})
		return
	}

	// Вложенный Спан: Получение метаданных из БД
	var avatar *model.Avatar
	err := func() error {
		_, dbSpan := tracer.Start(ctx, "Database:GetAvatarByID", trace.WithSpanKind(trace.SpanKindClient))
		defer dbSpan.End()

		var dbErr error
		avatar, dbErr = h.repo.GetByID(ctx, avatarID)
		return dbErr
	}()

	if err != nil {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "avatar_not_found")))
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
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "size_not_processed")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Requested size not processed yet"})
		return
	}
	span.SetAttributes(attribute.String("avatar.s3_key", objectKey))

	// Вложенный Спан: Запрос объекта из S3 MinIO
	var object *minio.Object
	err = func() error {
		_, s3GetSpan := tracer.Start(ctx, "Minio:GetObject", trace.WithSpanKind(trace.SpanKindClient))
		defer s3GetSpan.End()

		var s3Err error
		object, s3Err = h.s3.GetObject(ctx, BucketName, objectKey, minio.GetObjectOptions{})
		return s3Err
	}()

	if err != nil {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "s3_get_failed")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to retrieve image from storage"})
		return
	}
	defer object.Close()

	// Вложенный Спан: Получение Stat() из S3
	var objInfo minio.ObjectInfo
	err = func() error {
		_, s3StatSpan := tracer.Start(ctx, "Minio:StatObject", trace.WithSpanKind(trace.SpanKindClient))
		defer s3StatSpan.End()

		var statErr error
		objInfo, statErr = object.Stat()
		return statErr
	}()

	if err != nil {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "file_not_found_in_s3")))
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

	// Вложенный Спан: Стриминг байт клиенту
	var written int64
	err = func() error {
		_, ioSpan := tracer.Start(ctx, "HTTP:StreamBytesToClient", trace.WithSpanKind(trace.SpanKindClient))
		defer ioSpan.End()

		var ioErr error
		written, ioErr = io.Copy(w, object)
		return ioErr
	}()

	if err != nil {
		h.logger.ErrorContext(ctx, "error while copying from s3 object to response", "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "streaming interrupted")
		return
	}

	// Фиксируем успешные метрики
	h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", "success"),
		attribute.String("size_variant", size),
	))
	h.metrics.DownloadSizeHist.Record(ctx, float64(written), metric.WithAttributes(
		attribute.String("content_type", contentType),
	))

	span.SetStatus(codes.Ok, "Avatar served successfully")
}

// GET /api/v1/avatars/{avatar_id}/metadata
func (h *AvatarHandler) GetAvatarMetadataHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "GetAvatarMetadataHandler", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	avatarID := chi.URLParam(r, "avatar_id")
	if avatarID == "" {
		h.metrics.MetadataCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "missing_avatar_id")))
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing avatar_id parameter"})
		return
	}
	span.SetAttributes(attribute.String("avatar.id", avatarID))

	// Вложенный Спан: Чтение метаданных из БД
	var avatar *model.Avatar
	err := func() error {
		_, dbSpan := tracer.Start(ctx, "Database:GetAvatarMetadata", trace.WithSpanKind(trace.SpanKindClient))
		defer dbSpan.End()

		var dbErr error
		avatar, dbErr = h.repo.GetByID(ctx, avatarID)
		return dbErr
	}()

	if err != nil {
		h.metrics.MetadataCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "not_found")))
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

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(AvatarMetadataResponse{
		ID:         avatar.UUID,
		UserID:     avatar.UserID,
		FileName:   avatar.FileName,
		MimeType:   avatar.MimeType,
		Size:       avatar.SizeBytes,
		Dimensions: Dimensions{Width: avatar.Width, Height: avatar.Height},
		Thumbnails: thumbnails,
		CreatedAt:  avatar.CreatedAt,
		UpdatedAt:  avatar.UpdatedAt,
	})

	h.metrics.MetadataCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success")))
	span.SetStatus(codes.Ok, "Metadata served successfully")
}

// GET /api/v1/users/{user_id}/avatar
func (h *AvatarHandler) GetUserAvatarHandler(w http.ResponseWriter, r *http.Request) {
	ctx, span := tracer.Start(r.Context(), "GetUserAvatarHandler", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()

	userID := chi.URLParam(r, "user_id")
	if userID == "" {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "missing_user_id")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing user_id parameter"})
		return
	}
	span.SetAttributes(attribute.String("user.id", userID))

	size := r.URL.Query().Get("size")
	if size == "" {
		size = "original"
	}
	format := r.URL.Query().Get("format")

	span.SetAttributes(attribute.String("requested.size", size), attribute.String("requested.format", format))

	if size != "100x100" && size != "300x300" && size != "original" {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "invalid_size")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid size parameter"})
		return
	}

	if format != "" && format != "jpeg" && format != "png" && format != "webp" {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "invalid_format")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Invalid format parameter"})
		return
	}

	// Вложенный Спан: Поиск последней активной аватарки по UserID в БД
	var avatar *model.Avatar
	err := func() error {
		_, dbSpan := tracer.Start(ctx, "Database:GetAvatarByUserID",
			trace.WithSpanKind(trace.SpanKindClient))
		defer dbSpan.End()

		var dbErr error
		avatar, dbErr = h.repo.GetByUserID(ctx, userID)
		return dbErr
	}()

	if err != nil {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "user_avatar_not_found")))
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
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "size_not_processed")))

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Requested size not processed yet"})
		return
	}
	span.SetAttributes(attribute.String("avatar.s3_key", objectKey), attribute.String("avatar.id", avatar.UUID))

	// Вложенный Спан: Запрос картинки из MinIO
	var object *minio.Object
	err = func() error {
		_, s3GetSpan := tracer.Start(ctx, "Minio:GetObject", trace.WithSpanKind(trace.SpanKindClient))
		defer s3GetSpan.End()
		var s3Err error
		object, s3Err = h.s3.GetObject(ctx, BucketName, objectKey, minio.GetObjectOptions{})
		return s3Err
	}()
	if err != nil {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "s3_get_failed")))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to retrieve image from storage"})
		return
	}
	defer object.Close()

	// Вложенный Спан: Чтение заголовков Stat() из S3
	var objInfo minio.ObjectInfo
	err = func() error {
		_, s3StatSpan := tracer.Start(ctx, "Minio:StatObject", trace.WithSpanKind(trace.SpanKindClient))
		defer s3StatSpan.End()
		var statErr error
		objInfo, statErr = object.Stat()
		return statErr
	}()
	if err != nil {
		h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error"), attribute.String("reason", "file_not_found_in_s3")))
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
		w.Header().Set("ETag", fmt.Sprintf("%s", strings.Trim(objInfo.ETag, "")))
	}
	w.WriteHeader(http.StatusOK)

	// Вложенный Спан: Копирование потока данных в HTTP ответ
	var written int64
	err = func() error {
		_, ioSpan := tracer.Start(ctx, "HTTP:StreamBytesToClient", trace.WithSpanKind(trace.SpanKindClient))
		defer ioSpan.End()
		var ioErr error
		written, ioErr = io.Copy(w, object)
		return ioErr
	}()
	if err != nil {
		h.logger.ErrorContext(ctx, "error while copying from s3 object to response", "err", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "streaming interrupted")
		return
	}

	// Метрики успешного скачивания
	h.metrics.DownloadCounter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "success"), attribute.String("size_variant", size)))
	h.metrics.DownloadSizeHist.Record(ctx, float64(written), metric.WithAttributes(attribute.String("content_type", contentType)))
	span.SetStatus(codes.Ok, "User avatar served successfully")
}
