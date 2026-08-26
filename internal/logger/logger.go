package logger

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// глобальный логгер server
var Log *slog.Logger

func newOtelResource(ctx context.Context) (*resource.Resource, error) {
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithOS(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String("gophprofileservice"),
			semconv.ServiceVersionKey.String("1.0.0"),
			attribute.String("environment", os.Getenv("GO_ENV")),
		),
	)
	return res, err
}

func InitMeterProvider(ctx context.Context) func() {
	// Создаём OTel Exporter gRPC для метрик
	exporter, err := otlpmetricgrpc.New(ctx)

	if err != nil {
		log.Fatalf("failed to create OTLP exporter: %v", err)
	}

	// Добавляем метаинформацию о сервисе
	res, err := newOtelResource(ctx)
	if err != nil {
		log.Fatalf("failed to create meter resource: %v", err)
	}

	// Инициализируем MeterProvider
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(
			metric.NewPeriodicReader(
				exporter,
				metric.WithInterval(2*time.Second),
			),
		),
	)
	otel.SetMeterProvider(meterProvider)

	return func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()

		if err := meterProvider.Shutdown(ctx); err != nil {
			otel.Handle(err)
		}
	}
}

func InitLoggerProvider(ctx context.Context) (*slog.Logger, func()) {
	// Создаём gRPC Exporter для логов
	exporter, err := otlploggrpc.New(ctx)
	if err != nil {
		log.Fatalf("failed to create OTLP log exporter: %v", err)
	}

	// Метаинформация (Resource)
	res, err := newOtelResource(ctx)
	if err != nil {
		log.Fatalf("failed to create logger resource: %v", err)
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

// InitTraceProvider настраивает сбор трейсов через gRPC OTLP экспортер
func InitTraceProvider(ctx context.Context) func() {
	// Создаем OTel Exporter для трейсов по gRPC
	exporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		log.Fatalf("failed to create OTLP trace exporter: %v", err)
	}

	// Добавляем метаинформацию о сервисе (абсолютно идентичную InitMeterProvider)
	res, err := newOtelResource(ctx)
	if err != nil {
		log.Fatalf("failed to create trace resource: %v", err)
	}

	// Инициализируем TracerProvider с пакетным процессором (BatchSpanProcessor)
	bsp := trace.NewBatchSpanProcessor(exporter)

	tracerProvider := trace.NewTracerProvider(
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(res),
		trace.WithSpanProcessor(bsp),
	)

	// Делаем этот провайдер глобальным для всего Go-приложения
	otel.SetTracerProvider(tracerProvider)

	// Возвращаем функцию плавного закрытия (Graceful Shutdown)
	return func() {
		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		if err := tracerProvider.Shutdown(ctx); err != nil {
			otel.Handle(err)
		}
	}
}
