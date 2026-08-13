package handler

import (
	"database/sql"
	"encoding/json"
	"errors"
	"gophprofile/internal/config"
	"gophprofile/internal/model"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// DELETE /api/v1/avatars/{avatar_id}
func (h *AvatarHandler) DeleteAvatarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Начинаем замер времени выполнения операции
	startTime := time.Now()

	// Создаем корневой спан для операции удаления
	ctx, span := tracer.Start(r.Context(), "DeleteAvatarHandler",
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "missing_user_id"),
		))
		span.SetStatus(codes.Error, "Missing X-User-ID header")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing X-User-ID header"})
		return
	}
	span.SetAttributes(attribute.String("user.id", userID))

	avatarID := chi.URLParam(r, "avatar_id")
	if avatarID == "" {
		h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "missing_avatar_id"),
		))
		span.SetStatus(codes.Error, "Missing avatar_id parameter")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing avatar_id parameter"})
		return
	}
	span.SetAttributes(attribute.String("avatar.id", avatarID))

	h.logger.InfoContext(ctx, "Processing DeleteAvatar request", "avatar_id", avatarID, "user_id", userID)

	// Вложенный Спан: Быстрый GetByID для проверки прав (owner)
	var avatar *model.Avatar
	err := func() error {
		_, dbGetSpan := tracer.Start(ctx, "Database:GetAvatarMetadata", trace.WithSpanKind(trace.SpanKindClient))
		defer dbGetSpan.End()

		var dbErr error
		avatar, dbErr = h.repo.GetByID(ctx, avatarID)
		if dbErr != nil {
			dbGetSpan.RecordError(dbErr)
			dbGetSpan.SetStatus(codes.Error, "Database fetch error")
		}
		return dbErr
	}()

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("status", "error"),
				attribute.String("reason", "avatar_not_found"),
			))
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar not found"})
			return
		}
		h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "db_fetch_failed"),
		))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to fetch avatar"})
		return
	}

	// Проверяем права владения ресурсом
	if avatar.UserID != userID {
		h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "forbidden_not_owner"),
		))
		span.SetStatus(codes.Error, "Forbidden: User is not the owner of the avatar")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "Forbidden",
			Details: "You can only delete your own avatars",
		})
		return
	}

	// Вложенный Спан: Мягкое удаление в БД
	var deletedAvatar *model.Avatar
	err = func() error {
		_, dbDeleteSpan := tracer.Start(ctx, "Database:SoftDeleteAvatar", trace.WithSpanKind(trace.SpanKindClient))
		defer dbDeleteSpan.End()

		var dbErr error
		deletedAvatar, dbErr = h.repo.SoftDelete(ctx, avatarID)
		if dbErr != nil {
			dbDeleteSpan.RecordError(dbErr)
			dbDeleteSpan.SetStatus(codes.Error, "Database soft-delete execution error")
		}
		return dbErr
	}()

	if err != nil {
		h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
			attribute.String("reason", "db_soft_delete_failed"),
		))
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to execute soft delete"})
		return
	}

	// Ссылки собираем в один массив для воркера
	var keysToDelete []string
	if deletedAvatar.S3Key != "" {
		keysToDelete = append(keysToDelete, deletedAvatar.S3Key)
	}
	if deletedAvatar.Thumbnail_S3_Keys.Small != "" {
		keysToDelete = append(keysToDelete, deletedAvatar.Thumbnail_S3_Keys.Small)
	}
	if deletedAvatar.Thumbnail_S3_Keys.Medium != "" {
		keysToDelete = append(keysToDelete, deletedAvatar.Thumbnail_S3_Keys.Medium)
	}

	span.SetAttributes(attribute.Int("deleted.files_count", len(keysToDelete)))

	// Отправляем задачу на асинхронное удаление файлов в Kafka
	if len(keysToDelete) > 0 {
		task := model.AvatarDeleteTask{
			AvatarID: avatarID,
			S3Keys:   keysToDelete,
		}

		taskBytes, err := json.Marshal(task)
		if err != nil {
			h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("status", "error"),
				attribute.String("reason", "json_marshal_failed"),
			))
			span.RecordError(err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Internal task serialization error"})
			return
		}

		// Вложенный Спан: Отправка задачи в брокер Кафки
		err = func() error {
			_, kafkaSpan := tracer.Start(ctx, "Kafka:PublishDeleteTask", trace.WithSpanKind(trace.SpanKindProducer))
			defer kafkaSpan.End()

			return h.kafka.WriteMessages(ctx, kafka.Message{
				Topic: config.KafkaDeleteTopic,
				Key:   []byte(userID),
				Value: taskBytes,
			})
		}()

		if err != nil {
			h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
				attribute.String("status", "error"),
				attribute.String("reason", "kafka_publish_failed"),
			))
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to schedule background file deletion"})
			return
		}
	}

	// Считаем и фиксируем полную задержку успешной операции
	durationMs := time.Since(startTime).Seconds() * 1000

	// Записываем метрики успешного выполнения
	h.metrics.DeleteCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", "success"),
	))
	h.metrics.DeleteDurationHist.Record(ctx, durationMs, metric.WithAttributes(
		attribute.String("status", "success"),
	))

	w.WriteHeader(http.StatusNoContent)
	span.SetStatus(codes.Ok, "Avatar soft-deleted and purge task scheduled successfully")
}
