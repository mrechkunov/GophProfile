package handler_test

import (
	"encoding/json"
	"errors"
	"gophprofile/internal/handler"
	"gophprofile/internal/repository/mocks"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
)

func TestHealthCheckHandler_AllUp(t *testing.T) {
	// Изолируем сбор метрик OpenTelemetry
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик для Health-проб
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	// Инициализируем моки для всех внешних компонентов
	mockRepo := new(mocks.MockAvatarRepository)
	mockMinio := new(mocks.MockMinioClient)
	mockKafka := new(mocks.MockKafkaProducer)
	discardLogger := slog.New(slog.DiscardHandler)

	// Передаем все три мока и объект метрик в конструктор хэндлера
	h := handler.NewAvatarHandler(mockRepo, mockMinio, mockKafka, discardLogger, metrics)

	// Настраиваем успешное (зелёное) поведение для каждого мока
	mockRepo.On("Ping", mock.Anything).Return(nil)
	mockMinio.On("BucketExists", mock.Anything, handler.BucketName).Return(true, nil)
	mockKafka.On("Ping", mock.Anything).Return(nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	// Act
	h.HealthCheckHandler(rr, req)

	// Assert: Проверяем, что теперь мы железно получаем 200 OK
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.HealthResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)

	assert.Equal(t, "UP", res.Status)
	assert.Equal(t, "UP", res.Components["database"].Status)
	assert.Equal(t, "UP", res.Components["storage"].Status)
	assert.Equal(t, "UP", res.Components["broker"].Status)

	// Проверяем, что все моки были вызваны тестом
	mockRepo.AssertExpectations(t)
	mockMinio.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}

func TestHealthCheckHandler_ComponentDown_Returns503(t *testing.T) {
	// Изолируем сбор метрик OpenTelemetry для теста
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	// Инициализируем моки компонентов
	mockRepo := new(mocks.MockAvatarRepository)
	mockMinio := new(mocks.MockMinioClient)
	mockKafka := new(mocks.MockKafkaProducer)
	discardLogger := slog.New(slog.DiscardHandler)

	// Собираем хэндлер с зависимостями и объектом метрик
	h := handler.NewAvatarHandler(mockRepo, mockMinio, mockKafka, discardLogger, metrics)

	// Настраиваем негативное поведение для Базы Данных (возвращаем ошибку коннекта)
	mockRepo.On("Ping", mock.Anything).Return(errors.New("dial tcp 127.0.0.1:5432: connect: connection refused"))

	// Хранилище и Брокер работают исправно
	mockMinio.On("BucketExists", mock.Anything, handler.BucketName).Return(true, nil)
	mockKafka.On("Ping", mock.Anything).Return(nil)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()

	// Act
	h.HealthCheckHandler(rr, req)

	// Assert: Проверяем, что при падении хотя бы одного компонента отдается 503 статус
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("Content-Type"))

	var res handler.HealthResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)

	// Глобальный статус всей системы должен переключиться в DOWN
	assert.Equal(t, "DOWN", res.Status)

	// База данных должна быть DOWN и содержать описание ошибки
	assert.Equal(t, "DOWN", res.Components["database"].Status)
	assert.Contains(t, res.Components["database"].Error, "connection refused")

	// Остальные компоненты при этом остаются в статусе UP
	assert.Equal(t, "UP", res.Components["storage"].Status)
	assert.Equal(t, "UP", res.Components["broker"].Status)

	// Проверяем, что все моки были вызваны тестом
	mockRepo.AssertExpectations(t)
	mockMinio.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}
