package logger

import (
	"context"
	"log"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// глобальный логгер
var Log *slog.Logger
var OtelShutdown func()

func InitLoggerProvider(ctx context.Context) (*slog.Logger, func()) {
	// Создаём gRPC Exporter для логов (порт 4317)
	exporter, err := otlploggrpc.New(ctx)
	if err != nil {
		log.Fatalf("failed to create OTLP log exporter: %v", err)
	}

	// Метаинформация (Resource)
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("gophprofileservice"),
			semconv.ServiceVersionKey.String("1.0.0"),
		),
	)
	if err != nil {
		log.Fatalf("failed to create resource: %v", err)
	}

	// Инициализируем LoggerProvider
	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
	)

	// Создаем slog Handler через otelslog bridge
	handler := otelslog.NewHandler(
		"gophprofileservice",
		otelslog.WithLoggerProvider(loggerProvider),
	)

	// Создаем slog логгер с этим handler'ом
	logger := slog.New(handler)

	// Устанавливаем как глобальный логгер
	slog.SetDefault(logger)

	// Возвращаем функцию для корректного завершения (flush данных перед выходом)
	shutdown := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := loggerProvider.Shutdown(ctx); err != nil {
			otel.Handle(err)
		}
	}

	return logger, shutdown
}
