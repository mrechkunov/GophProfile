package handler_test

import (
	"encoding/json"
	"gophprofile/internal/handler"
	"gophprofile/internal/repository"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestHealthCheckHandler_AllUp(t *testing.T) {
	// Инициализируем моки для ВСЕХ трех компонентов
	mockRepo := new(repository.MockAvatarRepository)
	mockMinio := new(repository.MockMinioClient)
	mockKafka := new(repository.MockKafkaProducer) // Добавили мок кафки

	// Передаем все три мока в хэндлер
	h := handler.NewAvatarHandler(mockRepo, mockMinio, mockKafka)

	// Настраиваем успешное (зелёное) поведение для каждого мока
	mockRepo.On("Ping", mock.Anything).Return(nil)
	mockMinio.On("BucketExists", mock.Anything, handler.BucketName).Return(true, nil)
	mockKafka.On("Ping", mock.Anything).Return(nil) // Мокаем успешный ответ Кафки в памяти

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	// Act
	h.HealthCheckHandler(rr, req)

	// Assert: Проверяем, что теперь мы железно получаем 200 OK
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.HealthResponse
	json.Unmarshal(rr.Body.Bytes(), &res)

	assert.Equal(t, "UP", res.Status)
	assert.Equal(t, "UP", res.Components["database"].Status)
	assert.Equal(t, "UP", res.Components["storage"].Status)
	assert.Equal(t, "UP", res.Components["broker"].Status) // Проверяем, что брокер тоже UP

	// Проверяем, что все моки были вызваны тестом
	mockRepo.AssertExpectations(t)
	mockMinio.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}
