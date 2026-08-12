package handler_test

import (
	"context"
	"gophprofile/internal/handler"
	"gophprofile/internal/model"
	"gophprofile/internal/repository/mocks"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDeleteAvatarHandler_Async_Success(t *testing.T) {
	mockRepo := new(mocks.MockAvatarRepository)
	mockKafka := new(mocks.MockKafkaProducer)
	discardLogger := slog.New(slog.DiscardHandler)
	// MinIO здесь передаем как nil
	h := handler.NewAvatarHandler(mockRepo, nil, mockKafka, discardLogger)

	dbAvatar := &model.Avatar{
		UUID:   "avatar-async-999",
		UserID: "user-alpha",
		S3Key:  "originals/pic.png",
		Thumbnail_S3_Keys: model.Thumbnails{
			Small: "resized/100_pic.png",
		},
	}

	// Настраиваем поведение моков
	mockRepo.On("GetByID", mock.Anything, "avatar-async-999").Return(dbAvatar, nil)
	mockRepo.On("SoftDelete", mock.Anything, "avatar-async-999").Return(dbAvatar, nil)
	mockKafka.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/avatars/avatar-async-999", nil)
	req.Header.Set("X-User-ID", "user-alpha")

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("avatar_id", "avatar-async-999")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	h.DeleteAvatarHandler(rr, req)

	// Убеждаемся, что статус ответа 204 и тело пустое
	assert.Equal(t, http.StatusNoContent, rr.Code)
	assert.Empty(t, rr.Body.Bytes())

	// Проверяем, что все асинхронные шаги (БД + Kafka) были выполнены
	mockRepo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}
