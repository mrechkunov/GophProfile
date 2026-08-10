package repository

import (
	"context"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/mock"
)

// KafkaProducerAPI описывает отправку сообщений в Kafka
type KafkaProducerAPI interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Ping(ctx context.Context) error
}

type KafkaProducer struct {
	Writer *kafka.Writer
}

func (k *KafkaProducer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	return k.Writer.WriteMessages(ctx, msgs...)
}
func (k *KafkaProducer) Ping(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", k.Writer.Addr.String())
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

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
