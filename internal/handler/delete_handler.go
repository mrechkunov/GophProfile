package handler

import (
	"encoding/json"
	"gophprofile/internal/config"
	"gophprofile/internal/model"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/segmentio/kafka-go"
)

// DELETE /api/v1/avatars/{avatar_id}
func (h *AvatarHandler) DeleteAvatarHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodDelete {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	userID := r.Header.Get("X-User-ID")
	if userID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing X-User-ID header"})
		return
	}

	avatarID := chi.URLParam(r, "avatar_id")
	if avatarID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Missing avatar_id parameter"})
		return
	}

	// Делаем быстрый GetByID, чтобы проверить права (owner) до изменения состояния бд
	avatar, err := h.repo.GetByID(r.Context(), avatarID)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Avatar not found"})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to fetch avatar"})
		return
	}

	if avatar.UserID != userID {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(ErrorResponse{
			Error:   "Forbidden",
			Details: "You can only delete your own avatars",
		})
		return
	}

	// Делаем мягкое удаление в БД и забираем актуальные ключи
	deletedAvatar, err := h.repo.SoftDelete(r.Context(), avatarID)
	if err != nil {
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

	// Отправляем задачу на асинхронное удаление файлов в Kafka
	if len(keysToDelete) > 0 {
		task := model.AvatarDeleteTask{
			AvatarID: avatarID,
			S3Keys:   keysToDelete,
		}

		taskBytes, err := json.Marshal(task)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Internal task serialization error"})
			return
		}

		err = h.kafka.WriteMessages(r.Context(), kafka.Message{
			Topic: config.KafkaDeleteTopic,
			Key:   []byte(userID),
			Value: taskBytes,
		})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(ErrorResponse{Error: "Failed to schedule background file deletion"})
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
