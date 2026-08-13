package handler_test

import (
	"context"
	"encoding/json"
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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
)

func TestDeleteAvatarHandler_Forbidden_NotOwner(t *testing.T) {
	// Изолируем сбор метрик OpenTelemetry для теста
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	mockRepo := new(mocks.MockAvatarRepository)
	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(mockRepo, nil, nil, discardLogger, metrics)

	avatarID := "avatar-123"

	// Mock: БД возвращает аватарку, но она принадлежит пользователю "user-owner"
	mockRepo.On("GetByID", mock.Anything, avatarID).Return(&model.Avatar{
		UUID:   avatarID,
		UserID: "user-owner",
	}, nil)

	// Формируем запрос от имени другого пользователя "wrong-user"
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/avatars/"+avatarID, nil)
	req.Header.Set("X-User-ID", "wrong-user")

	chiCtx := chi.NewRouteContext()
	chiCtx.URLParams.Add("avatar_id", avatarID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, chiCtx))

	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.DeleteAvatarHandler(rr, req)

	// Проверяем результаты валидации прав
	assert.Equal(t, http.StatusForbidden, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, "Forbidden", res.Error)

	// Гарантируем, что метод SoftDelete НЕ БЫЛ вызван (удаление заблокировано)
	mockRepo.AssertNotCalled(t, "SoftDelete", mock.Anything, mock.Anything)
	mockRepo.AssertExpectations(t)
}
func TestDeleteAvatarHandler_Success(t *testing.T) {
	// Изолируем OpenTelemetry метрики
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	mockRepo := new(mocks.MockAvatarRepository)
	mockKafka := new(mocks.MockKafkaProducer)
	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(mockRepo, nil, mockKafka, discardLogger, metrics)

	avatarID := "my-avatar-id"
	userID := "user-right-owner"

	// Mock 1: Первичная проверка прав — возвращаем модель аватара
	mockRepo.On("GetByID", mock.Anything, avatarID).Return(&model.Avatar{
		UUID:   avatarID,
		UserID: userID,
	}, nil)

	// Mock 2: Мягкое удаление возвращает обновленную запись с ключами для Kafka
	mockRepo.On("SoftDelete", mock.Anything, avatarID).Return(&model.Avatar{
		UUID:   avatarID,
		UserID: userID,
		S3Key:  "originals/my-avatar-id.png",
		Thumbnail_S3_Keys: model.Thumbnails{
			Small:  "thumbnails/my-avatar-id_100.png",
			Medium: "thumbnails/my-avatar-id_300.png",
		},
	}, nil)

	// Mock 3: Ожидаем успешную отправку задачи на удаление файлов в шину Kafka
	mockKafka.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	// Формируем легитимный запрос от настоящего владельца
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/avatars/"+avatarID, nil)
	req.Header.Set("X-User-ID", userID)

	chiCtx := chi.NewRouteContext()
	chiCtx.URLParams.Add("avatar_id", avatarID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, chiCtx))

	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.DeleteAvatarHandler(rr, req)

	// Проверяем HTTP статус успешного удаления без контента (Стандарт REST API)
	assert.Equal(t, http.StatusNoContent, rr.Code)

	// Проверяем, что вся цепочка вызовов к БД и брокеру очередей успешно отработала
	mockRepo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}
