package handler_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gophprofile/internal/handler"
	"gophprofile/internal/model"
	"gophprofile/internal/repository/mocks"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
)

// Вспомогательная функция для генерации валидного multipart тела (PNG картинка)
func createValidMultipartBody(t *testing.T, fieldName, fileName string, size int) (*bytes.Buffer, string) {
	pngMagicBytes := []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile(fieldName, fileName)
	assert.NoError(t, err)

	_, err = part.Write(pngMagicBytes)
	assert.NoError(t, err)

	if size > len(pngMagicBytes) {
		extra := make([]byte, size-len(pngMagicBytes))
		_, err = part.Write(extra)
		assert.NoError(t, err)
	}

	err = writer.Close()
	assert.NoError(t, err)

	return body, writer.FormDataContentType()
}

func TestPostUploadAvatarHandler_MissingUserID(t *testing.T) {
	// Настраиваем тестовый MeterProvider для изоляции метрик
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем метрики, так как хендлер запишет туда ошибку
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger, metrics)

	// Создаем запрос БЕЗ заголовка X-User-ID
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", nil)
	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.PostUploadAvatarHandler(rr, req)

	// Проверяем, что хендлер вернул 400 Bad Request
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Проверяем JSON-структуру ответа на ошибку
	var resp map[string]string
	err = json.Unmarshal(rr.Body.Bytes(), &resp)
	assert.NoError(t, err)
	assert.Equal(t, "Missing X-User-ID header", resp["error"])
}

func TestPostUploadAvatarHandler_MultipartBodyTooLarge(t *testing.T) {
	// Настраиваем тестовый MeterProvider для изоляции метрик
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем метрики, так как хендлер будет писать туда ошибку
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger, metrics)

	// Передаем размер больше константы MaxFileSize (10 * 1024 * 1024)
	body, contentType := createValidMultipartBody(t, "image", "avatar.png", handler.MaxFileSize+100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.PostUploadAvatarHandler(rr, req)

	// Проверяем HTTP статус и JSON ответ
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)

	var res handler.SizeErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)

	assert.Equal(t, "File too large", res.Error)
	assert.Equal(t, int64(handler.MaxFileSize), res.MaxSize)
}

func TestPostUploadAvatarHandler_MissingFileField(t *testing.T) {
	// Изолируем метрики OpenTelemetry для теста
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	discardLogger := slog.New(slog.DiscardHandler)

	// Передаем объект метрик пятым аргументом
	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger, metrics)

	// Создаем форму с неверным именем поля "wrong_field"
	body, contentType := createValidMultipartBody(t, "wrong_field", "avatar.png", 100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.PostUploadAvatarHandler(rr, req)

	// Проверяем код ответа и JSON
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, "Missing file field", res.Error)
}

func TestPostUploadAvatarHandler_InvalidMagicBytes(t *testing.T) {
	// Изолируем метрики OpenTelemetry для теста
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger, metrics)

	// Формируем multipart-тело, где файл называется test.png, но внутри обычный текст
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("image", "test.png")
	assert.NoError(t, err)

	_, err = part.Write([]byte("this is plain text data, not an image layout!"))
	assert.NoError(t, err)
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.PostUploadAvatarHandler(rr, req)

	// Проверяем код ответа и структуру ошибки
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, "Invalid file format", res.Error)
}

func TestPostUploadAvatarHandler_InvalidExtension(t *testing.T) {
	// Настраиваем тестовый MeterProvider для изоляции метрик
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger, metrics)

	// Генерируем валидные байты PNG, но сохраняем под расширением .exe
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("image", "malicious.exe")
	assert.NoError(t, err)

	// Пишем реальную картинку, чтобы пройти проверку http.DetectContentType
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	err = png.Encode(part, img)
	assert.NoError(t, err)
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.PostUploadAvatarHandler(rr, req)

	// Проверяем код ответа и структуру ошибки
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, "Invalid file extension", res.Error)
}

// Сбой загрузки в MinIO (Должен вернуть 500 ошибку, откат ресурсов не требуется)
func TestPostUploadAvatarHandler_MinioUploadError(t *testing.T) {
	// Настраиваем тестовый MeterProvider для изоляции метрик
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	// Инициализируем мок для S3
	mockS3 := new(mocks.MockMinioClient)
	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(nil, mockS3, nil, discardLogger, metrics)

	// Имитируем сбой сети или недоступность MinIO при попытке загрузки
	mockS3.On("PutObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(minio.UploadInfo{}, errors.New("s3 connection down"))

	// Генерируем реальное PNG изображение 1x1 пиксель в памяти,
	// чтобы хендлер успешно прошел image.DecodeConfig и дошел до логики PutObject
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	var imgBuffer bytes.Buffer
	err = png.Encode(&imgBuffer, img)
	assert.NoError(t, err)

	// Собираем правильный multipart/form-data body вручную с полем "image"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("image", "avatar.png")
	assert.NoError(t, err)
	_, err = part.Write(imgBuffer.Bytes())
	assert.NoError(t, err)
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	// Вызываем хендлер
	h.PostUploadAvatarHandler(rr, req)

	// Проверяем код ответа и структуру ошибки
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)

	// Проверяем текст ошибки, возвращаемый клиенту
	assert.Equal(t, "Failed to save file to storage", res.Error)

	// Убеждаемся, что метод PutObject действительно вызывался
	mockS3.AssertExpectations(t)
}

// Сбой сохранения в БД (ROLLBACK: файл должен удалиться из MinIO)
func TestPostUploadAvatarHandler_DBInsertionError_RollbackS3(t *testing.T) {
	// Настраиваем тестовый MeterProvider для изоляции метрик
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	// Инициализируем локальные моки
	mockRepo := new(mocks.MockAvatarRepository)
	mockS3 := new(mocks.MockMinioClient)
	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(mockRepo, mockS3, nil, discardLogger, metrics)

	// Успешная загрузка оригинального файла в S3
	mockS3.On("PutObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(minio.UploadInfo{}, nil)

	// Сбой при записи метаданных в БД
	mockRepo.On("Create", mock.Anything, mock.Anything).
		Return(errors.New("postgres connection lost"))

	// ОЖИДАЕМ ОТКАТ: Удаление объекта из MinIO в блоке defer
	mockS3.On("RemoveObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything).
		Return(nil)

	// Генерируем минимальное изображение для успешного парсинга размеров
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	var imgBuffer bytes.Buffer
	err = png.Encode(&imgBuffer, img)
	assert.NoError(t, err)

	// Собираем правильный multipart-body вручную с полем "image"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("image", "avatar.png")
	assert.NoError(t, err)
	_, err = part.Write(imgBuffer.Bytes())
	assert.NoError(t, err)
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-123")
	rr := httptest.NewRecorder()

	// Вызов тестируемого хендлера
	h.PostUploadAvatarHandler(rr, req)

	// Валидация результатов
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, "Failed to save avatar metadata", res.Error)

	// Проверяем, что триггер RemoveObject сработал в defer для отката S3
	mockS3.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
}

// Сбой отправки задачи в Kafka (FULL ROLLBACK: удаление из MinIO + удаление строки из БД)
func TestPostUploadAvatarHandler_KafkaError_FullRollback(t *testing.T) {
	// Настраиваем тестовый MeterProvider для изоляции метрик
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик нашего хендлера
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	// Инициализируем локальные моки
	mockRepo := new(mocks.MockAvatarRepository)
	mockS3 := new(mocks.MockMinioClient)
	mockKafka := new(mocks.MockKafkaProducer)
	discardLogger := slog.New(slog.DiscardHandler)

	// Собираем хендлер со всеми зависимостями и метриками
	h := handler.NewAvatarHandler(mockRepo, mockS3, mockKafka, discardLogger, metrics)

	// MinIO принимает файл успешно
	mockS3.On("PutObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(minio.UploadInfo{}, nil)

	// БД успешно создает запись метаданных
	mockRepo.On("Create", mock.Anything, mock.Anything).
		Return(nil)

	// Kafka падает с ошибкой брокера очередей
	mockKafka.On("WriteMessages", mock.Anything, mock.Anything).
		Return(errors.New("kafka broker unavailable"))

		// ОЖИДАЕМ ПОЛНЫЙ ОТКАТ В БЛОКЕ DEFER:
	// Чистим физический файл из хранилища S3
	mockS3.On("RemoveObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything).
		Return(nil)

	mockRepo.On("SoftDelete", mock.Anything, mock.Anything).
		Return(&model.Avatar{UUID: "user-999"}, nil)

	// Генерируем реальное PNG изображение 1x1 пиксель в памяти
	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	var imgBuffer bytes.Buffer
	err = png.Encode(&imgBuffer, img)
	assert.NoError(t, err)

	// Собираем правильный multipart/form-data body вручную с полем "image"
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("image", "avatar.png")
	assert.NoError(t, err)
	_, err = part.Write(imgBuffer.Bytes())
	assert.NoError(t, err)
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-999")
	rr := httptest.NewRecorder()

	// Запуск хендлера
	h.PostUploadAvatarHandler(rr, req)

	// Проверка результатов
	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	// Проверяем тело JSON-ответа
	var res handler.ErrorResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.Equal(t, "failed to dispatch async task", res.Error)

	// Убеждаемся, что все ожидания по мокам (включая RemoveObject и SoftDelete в defer) успешно выполнились
	mockS3.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}

// Успешный сценарий (все системы работают штатно)
func TestPostUploadAvatarHandler_Success(t *testing.T) {
	// Настраиваем тестовый MeterProvider для сбора метрик успеха
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	// Инициализируем структуру метрик нашего хендлера
	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	// Инициализируем локальные моки зависимостей
	mockRepo := new(mocks.MockAvatarRepository)
	mockS3 := new(mocks.MockMinioClient)
	mockKafka := new(mocks.MockKafkaProducer)
	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(mockRepo, mockS3, mockKafka, discardLogger, metrics)

	var capturedAvatar *model.Avatar

	// Настраиваем ожидания моков
	mockS3.On("PutObject", mock.Anything, handler.BucketName, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)

	// Перехватываем структуру данных для валидации полей БД
	mockRepo.On("Create", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		capturedAvatar = args.Get(1).(*model.Avatar)
	}).Return(nil)

	mockKafka.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)

	// Генерируем реальное изображение 100x100 в памяти, чтобы пройти image.DecodeConfig
	img := image.NewRGBA(image.Rect(0, 0, 100, 100))
	var imgBuffer bytes.Buffer
	err = png.Encode(&imgBuffer, img)
	assert.NoError(t, err)

	// Собираем валидный multipart/form-data body вручную
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, err := writer.CreateFormFile("image", "profile_pic.png")
	assert.NoError(t, err)
	_, err = part.Write(imgBuffer.Bytes())
	assert.NoError(t, err)
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-identity-ok")
	rr := httptest.NewRecorder()

	// Вызов тестируемого хендлера
	h.PostUploadAvatarHandler(rr, req)

	// Валидация HTTP-ответа
	assert.Equal(t, http.StatusCreated, rr.Code)

	var res handler.AvatarResponse
	err = json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, err)
	assert.NotEmpty(t, res.ID)
	assert.Equal(t, "user-identity-ok", res.UserID)
	assert.Equal(t, "processing", res.Status)
	assert.True(t, strings.HasPrefix(res.URL, fmt.Sprintf("/%s/originals/", handler.BucketName)))

	// Проверяем, корректно ли была сформирована структура для записи в базу данных
	assert.NotNil(t, capturedAvatar)
	assert.Equal(t, "user-identity-ok", capturedAvatar.UserID)
	assert.Equal(t, "profile_pic.png", capturedAvatar.FileName)
	assert.Equal(t, "image/png", capturedAvatar.MimeType)
	assert.Equal(t, "uploading", capturedAvatar.UploadStatus)

	// Проверяем, что хендлер успешно вытащил размеры картинки 100x100 и записал в БД
	assert.Equal(t, 100, capturedAvatar.Width)
	assert.Equal(t, 100, capturedAvatar.Height)

	// При успехе триггеры отката сбрасываются, методы RemoveObject и SoftDelete вызываться НЕ ДОЛЖНЫ.
	mockS3.AssertNotCalled(t, "RemoveObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertNotCalled(t, "SoftDelete", mock.Anything, mock.Anything)

	mockS3.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}
