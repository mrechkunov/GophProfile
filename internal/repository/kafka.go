package repository

import (
	"context"

	"github.com/segmentio/kafka-go"
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
