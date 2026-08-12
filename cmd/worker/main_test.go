package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"gophprofile/internal/model"
	"gophprofile/internal/repository"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

//ТЕСТЫ РЕСАЙЗА (RESIZE)

func TestResizeProcessor_ProcessResizeTask_InvalidJSON(t *testing.T) {
	p := NewResizeProcessor(nil, nil, nil)
	err := p.ProcessResizeTask(context.Background(), []byte(`{broken-json`))
	assert.Error(t, err)
}

// ТЕСТЫ ОЧИСТКИ S3 (SOFT-DELETE CLEANER)
func TestAvatarDeleteWorker_ProcessDeleteTask_Success(t *testing.T) {
	mockMinio := new(repository.MockMinioClient)
	discardLogger := slog.New(slog.DiscardHandler)
	w := NewAvatarDeleteWorker(mockMinio, discardLogger)

	// Ожидаем физическое удаление файлов из S3
	mockMinio.On("RemoveObject", mock.Anything, BucketName, "originals/123.png", mock.Anything).Return(nil)
	mockMinio.On("RemoveObject", mock.Anything, BucketName, "minimals/123_100.png", mock.Anything).Return(nil)

	task := model.AvatarDeleteTask{
		AvatarID: "123",
		S3Keys:   []string{"originals/123.png", "minimals/123_100.png"},
	}
	payload, _ := json.Marshal(task)

	err := w.ProcessDeleteTask(context.Background(), payload)

	assert.NoError(t, err)
	mockMinio.AssertExpectations(t)
}

func TestAvatarDeleteWorker_ProcessDeleteTask_InvalidJSON(t *testing.T) {
	w := NewAvatarDeleteWorker(nil, nil)
	err := w.ProcessDeleteTask(context.Background(), []byte(`{invalid`))
	assert.Error(t, err)
}
