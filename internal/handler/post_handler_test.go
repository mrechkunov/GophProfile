package handler_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gophprofile/internal/handler"
	"gophprofile/internal/model"
	"gophprofile/internal/repository"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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

// ТЕСТОВЫЕ СЦЕНАРИИ

// Метод запроса не POST
func TestPostUploadAvatarHandler_MethodNotAllowed(t *testing.T) {
	h := handler.NewAvatarHandler(nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/avatars", nil)
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

// Отсутствует заголовок X-User-ID
func TestPostUploadAvatarHandler_MissingUserID(t *testing.T) {
	h := handler.NewAvatarHandler(nil, nil, nil)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", nil)
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Missing X-User-ID header", res.Error)
}

// Файл слишком большой на этапе парсинга Multipart формы (> 10MB)
func TestPostUploadAvatarHandler_MultipartBodyTooLarge(t *testing.T) {
	h := handler.NewAvatarHandler(nil, nil, nil)

	// Передаем размер больше константы MaxFileSize (10 * 1024 * 1024)
	body, contentType := createValidMultipartBody(t, "image", "avatar.png", handler.MaxFileSize+100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
	var res handler.SizeErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "File too large", res.Error)
	assert.Equal(t, int64(handler.MaxFileSize), res.MaxSize)
}

// Отсутствует нужное поле файла ("image") в форме
func TestPostUploadAvatarHandler_MissingFileField(t *testing.T) {
	h := handler.NewAvatarHandler(nil, nil, nil)
	// Создаем форму с неверным именем поля "wrong_field"
	body, contentType := createValidMultipartBody(t, "wrong_field", "avatar.png", 100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Missing file field", res.Error)
}

// Невалидный формат файла (проверка Magic Bytes на примере plain text)
func TestPostUploadAvatarHandler_InvalidMagicBytes(t *testing.T) {
	h := handler.NewAvatarHandler(nil, nil, nil)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	part, _ := writer.CreateFormFile("image", "test.png")
	part.Write([]byte("this is plain text data, not an image layout!"))
	writer.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Invalid file format", res.Error)
}

// Конфликт: Валидные Magic Bytes, но невалидное расширение файла (.exe)
func TestPostUploadAvatarHandler_InvalidExtension(t *testing.T) {
	h := handler.NewAvatarHandler(nil, nil, nil)
	body, contentType := createValidMultipartBody(t, "image", "malicious.exe", 100)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Invalid file extension", res.Error)
}

// Сбой загрузки в MinIO (Должен вернуть 500 ошибку, откат ресурсов не требуется)
func TestPostUploadAvatarHandler_MinioUploadError(t *testing.T) {
	mockMinio := new(repository.MockMinioClient)
	h := handler.NewAvatarHandler(nil, mockMinio, nil)

	mockMinio.On("PutObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(minio.UploadInfo{}, errors.New("s3 connection down"))

	body, contentType := createValidMultipartBody(t, "image", "avatar.png", 100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-1")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)
	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Failed to save file to storage", res.Error)
	mockMinio.AssertExpectations(t)
}

// Сбой сохранения в БД (ROLLBACK: файл должен удалиться из MinIO)
func TestPostUploadAvatarHandler_DBInsertionError_RollbackS3(t *testing.T) {
	mockRepo := new(repository.MockAvatarRepository)
	mockMinio := new(repository.MockMinioClient)
	h := handler.NewAvatarHandler(mockRepo, mockMinio, nil)

	// Успешная загрузка оригинального файла в S3
	mockMinio.On("PutObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(minio.UploadInfo{}, nil)

	// Сбой при записи метаданных в БД
	mockRepo.On("Create", mock.Anything, mock.Anything).
		Return(errors.New("postgres connection lost"))

	// ОЖИДАЕМ ОТКАТ: Удаление объекта из MinIO в блоке defer
	mockMinio.On("RemoveObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything).
		Return(nil)

	body, contentType := createValidMultipartBody(t, "image", "avatar.png", 100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-123")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.Equal(t, "Failed to save avatar metadata", res.Error)

	mockMinio.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
}

// Сбой отправки задачи в Kafka (FULL ROLLBACK: удаление из MinIO + удаление строки из БД)
func TestPostUploadAvatarHandler_KafkaError_FullRollback(t *testing.T) {
	mockRepo := new(repository.MockAvatarRepository)
	mockMinio := new(repository.MockMinioClient)
	mockKafka := new(repository.MockKafkaProducer)
	h := handler.NewAvatarHandler(mockRepo, mockMinio, mockKafka)

	// MinIO принимает файл
	mockMinio.On("PutObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(minio.UploadInfo{}, nil)

	// БД успешно создает запись
	mockRepo.On("Create", mock.Anything, mock.Anything).
		Return(nil)

	// Kafka падает с ошибкой брокера очередей
	mockKafka.On("WriteMessages", mock.Anything, mock.Anything).
		Return(errors.New("kafka broker unavailable"))

	// ОЖИДАЕМ ПОЛНЫЙ ОТКАТ В БЛОКЕ DEFER:
	// Чистим файл из хранилища S3
	mockMinio.On("RemoveObject", mock.Anything, handler.BucketName, mock.Anything, mock.Anything).
		Return(nil)

	// Делаем мягкое удаление созданной записи в БД.
	// Возвращаем пустой объект аватара и nil в качестве ошибки, чтобы defer выполнился без сбоев
	mockRepo.On("SoftDelete", mock.Anything, mock.Anything).
		Return(&model.Avatar{}, nil)

	body, contentType := createValidMultipartBody(t, "image", "avatar.png", 100)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-999")
	rr := httptest.NewRecorder()

	h.PostUploadAvatarHandler(rr, req)

	assert.Equal(t, http.StatusInternalServerError, rr.Code)

	var res handler.ErrorResponse
	json.Unmarshal(rr.Body.Bytes(), &res)
	assert.NoError(t, json.Unmarshal(rr.Body.Bytes(), &res))
	assert.Equal(t, "failed to dispatch async task", res.Error)

	mockMinio.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)
}

// Успешный сценарий (все системы работают штатно)
func TestPostUploadAvatarHandler_Success(t *testing.T) {
	mockRepo := new(repository.MockAvatarRepository)
	mockMinio := new(repository.MockMinioClient)
	mockKafka := new(repository.MockKafkaProducer)
	h := handler.NewAvatarHandler(mockRepo, mockMinio, mockKafka)
	var capturedAvatar *model.Avatar
	mockMinio.On("PutObject", mock.Anything, handler.BucketName, mock.Anything,
		mock.Anything, mock.Anything, mock.Anything).Return(minio.UploadInfo{}, nil)
	// Перехватываем структуру данных для валидации полей БД
	mockRepo.On("Create", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		capturedAvatar = args.Get(1).(*model.Avatar)
	}).Return(nil)

	mockKafka.On("WriteMessages", mock.Anything, mock.Anything).Return(nil)
	body, contentType := createValidMultipartBody(t, "image", "profile_pic.png", 500)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", body)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-User-ID", "user-identity-ok")
	rr := httptest.NewRecorder()
	h.PostUploadAvatarHandler(rr, req)
	// Проверяем HTTP статус успеха
	assert.Equal(t, http.StatusCreated, rr.Code)

	// Проверяем тело ответа хэндлера
	var res handler.AvatarResponse
	err := json.Unmarshal(rr.Body.Bytes(), &res)
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

	// При успехе триггеры отката сбрасываются в false,
	// поэтому методы RemoveObject и Delete вызываться НЕ ДОЛЖНЫ.
	mockMinio.AssertNotCalled(t, "RemoveObject", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	mockRepo.AssertNotCalled(t, "Delete", mock.Anything, mock.Anything)

	mockMinio.AssertExpectations(t)
	mockRepo.AssertExpectations(t)
	mockKafka.AssertExpectations(t)

}
