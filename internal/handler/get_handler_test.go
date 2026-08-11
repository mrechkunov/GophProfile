package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"gophprofile/internal/handler"
	"gophprofile/internal/model"
	"gophprofile/internal/repository"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Тест сценария 404: Аватарка не найдена в Базе Данных
func TestGetAvatarHandler_Binary_NotFound(t *testing.T) {
	mockRepo := new(repository.MockAvatarRepository)
	discardLogger := slog.New(slog.DiscardHandler)
	h := handler.NewAvatarHandler(mockRepo, nil, nil, discardLogger)

	// Настраиваем мок репозитория на возврат ошибки отсутствия строк
	mockRepo.On("GetByID", mock.Anything, "missing-avatar-id").
		Return(nil, errors.New("sql: no rows in result set"))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/missing-avatar-id?size=300x300&format=webp", nil)

	// Контекст Chi
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("avatar_id", "missing-avatar-id")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	h.GetAvatarHandler(rr, req)

	// Проверяем соответствие спецификации JSON ответа 404
	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Avatar not found", res.Error)

	mockRepo.AssertExpectations(t)
}

// Тест сценария 400: Передан неверный размер
func TestGetAvatarHandler_Binary_InvalidSize(t *testing.T) {
	discardLogger := slog.New(slog.DiscardHandler)
	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/some-id?size=999x999", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("avatar_id", "some-id")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()
	h.GetAvatarHandler(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetAvatarMetadataHandler_Success(t *testing.T) {
	mockRepo := new(repository.MockAvatarRepository)
	discardLogger := slog.New(slog.DiscardHandler)
	h := handler.NewAvatarHandler(mockRepo, nil, nil, discardLogger)

	now := time.Now().UTC()
	expectedAvatar := &model.Avatar{
		UUID:      "avatar-uuid-111",
		UserID:    "user-id-222",
		FileName:  "photo.jpg",
		MimeType:  "image/jpeg",
		SizeBytes: 1024000,
		S3Key:     "originals/avatar-uuid-111.jpg",
		Thumbnail_S3_Keys: model.Thumbnails{
			Small:  "resized/100x100_avatar.jpg",
			Medium: "resized/300x300_avatar.jpg",
		},
		ProcessingStatus: "completed",
		CreatedAt:        now,
		UpdatedAt:        now,
	}

	mockRepo.On("GetByID", mock.Anything, "avatar-uuid-111").Return(expectedAvatar, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars/avatar-uuid-111/metadata", nil)

	// Внедряем роутинг Chi в контекст запроса
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("avatar_id", "avatar-uuid-111")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	h.GetAvatarMetadataHandler(rr, req)

	// Проверки
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.AvatarMetadataResponse
	err := json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)

	assert.Equal(t, "avatar-uuid-111", res.ID)
	assert.Equal(t, "user-id-222", res.UserID)
	assert.Equal(t, "photo.jpg", res.FileName)
	assert.Equal(t, int64(1024000), res.Size)

	// Проверка вложенных структур
	assert.Equal(t, 1920, res.Dimensions.Width)
	assert.Len(t, res.Thumbnails, 2)
	assert.Equal(t, "100x100", res.Thumbnails[0].Size)
	assert.Contains(t, res.Thumbnails[0].URL, "resized/100x100_avatar.jpg")

	mockRepo.AssertExpectations(t)
}

func TestGetUserAvatarHandler_Binary_NotFound(t *testing.T) {
	mockRepo := new(repository.MockAvatarRepository)
	discardLogger := slog.New(slog.DiscardHandler)
	h := handler.NewAvatarHandler(mockRepo, nil, nil, discardLogger)

	// Настраиваем мок на возврат ошибки отсутствия записей для конкретного user_id
	mockRepo.On("GetByUserID", mock.Anything, "user-without-avatar").
		Return(nil, errors.New("sql: no rows in result set"))

	req := httptest.NewRequest(http.MethodGet, "/api/v1/users/user-without-avatar/avatar?size=100x100", nil)

	// Контекст Chi для user_id
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("user_id", "user-without-avatar")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rr := httptest.NewRecorder()

	h.GetUserAvatarHandler(rr, req)

	// Проверяем, что хэндлер отдал 404 JSON ответ
	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Avatar for this user not found", res.Error)

	mockRepo.AssertExpectations(t)
}
