package mocks

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

type MockKafkaProducer struct {
	mock.Mock
}

func (m *MockKafkaProducer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	// Принимаем вариативный аргумент через срез для удобства мокинга матчера testify
	args := m.Called(ctx, msgs)
	return args.Error(0)
}
func (m *MockKafkaProducer) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
