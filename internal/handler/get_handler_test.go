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

func TestGetAvatarHandler_SizeNotProcessedYet(t *testing.T) {
	// Изолируем OpenTelemetry
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	mockRepo := new(mocks.MockAvatarRepository)
	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(mockRepo, nil, nil, discardLogger, metrics)

	avatarID := "avatar-not-ready"

	// Mock: БД отдает метаданные, но поле ресайза Medium (300x300) еще ПУСТОЕ ("")
	mockRepo.On("GetByID", mock.Anything, avatarID).Return(&model.Avatar{
		UUID:     avatarID,
		UserID:   "user-2",
		MimeType: "image/jpeg",
		S3Key:    "originals/avatar-not-ready.jpg",
		Thumbnail_S3_Keys: model.Thumbnails{
			Small:  "thumbnails/avatar-not-ready_100.jpg",
			Medium: "", // Воркер асинхронного ресайза еще не обновил это поле
		},
	}, nil)

	// Клиент запрашивает именно 300x300
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/"+avatarID+"?size=300x300", nil)

	chiCtx := chi.NewRouteContext()
	chiCtx.URLParams.Add("avatar_id", avatarID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, chiCtx))

	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.GetAvatarHandler(rr, req)

	// Проверяем код ответа
	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)

	// Система должна честно ответить, что этот размер еще обрабатывается
	assert.Equal(t, "Requested size not processed yet", res.Error)

	mockRepo.AssertExpectations(t)
}
