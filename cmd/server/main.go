package main

import (
	"context"
	"errors"
	"gophprofile/internal/config"
	"gophprofile/internal/handler"
	"gophprofile/internal/logger"
	"gophprofile/internal/repository"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	// Создаем единый контекст для отслеживания системных сигналов
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	//  Инициализируем провайдер логов
	var otelLogsShutdown func()
	logger.Log, otelLogsShutdown = logger.InitLoggerProvider(ctx)
	defer func() {
		if otelLogsShutdown != nil {
			otelLogsShutdown()
		}
	}()

	//  Инициализируем провайдер трейсинга
	otelTracesShutdown := logger.InitTraceProvider(ctx)
	defer func() {
		if otelTracesShutdown != nil {
			otelTracesShutdown()
		}
	}()

	//  Инициализируем провайдер метрик
	otelMetricsShutdown := logger.InitMeterProvider(ctx)
	defer func() {
		if otelMetricsShutdown != nil {
			otelMetricsShutdown()
		}
	}()

	//  Инициализируем конфигурацию сервера
	config.InitServer(ctx)

	// Настраиваем зависимости и роутер
	router := chi.NewRouter()

	repo := repository.NewPostgresAvatarRepository(config.ConnServer.DB)
	kafkaAdapter := &repository.KafkaProducer{
		Writer: config.ConnServer.KafkaProducer,
	}

	wrappedHandler := otelhttp.NewHandler(
		router,
		"gophprofileservice",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	// Инициализируем метрики для хендлеров
	avatarMetrics, err := handler.NewAvatarMetrics()
	if err != nil {
		logger.Log.ErrorContext(ctx, "Не удалось инициализировать метрики хендлеров", "error", err)
	}

	avatarHandler := handler.NewAvatarHandler(
		repo,
		config.ConnServer.MinioClient,
		kafkaAdapter,
		logger.Log,
		avatarMetrics,
	)

	// Маршруты
	router.Get("/", avatarHandler.IndexHandler)
	router.Post("/api/v1/avatars", avatarHandler.PostUploadAvatarHandler)
	router.Get("/api/v1/avatars/{avatar_id}", avatarHandler.GetAvatarHandler)
	router.Get("/api/v1/users/{user_id}/avatar", avatarHandler.GetUserAvatarHandler)
	router.Get("/api/v1/avatars/{avatar_id}/metadata", avatarHandler.GetAvatarMetadataHandler)
	router.Delete("/api/v1/avatars/{avatar_id}", avatarHandler.DeleteAvatarHandler)
	router.Get("/health", avatarHandler.HealthCheckHandler)

	server := &http.Server{
		Addr:    config.CfgServer.Port,
		Handler: wrappedHandler,
	}

	logger.Log.Info("server starting", "port", config.CfgServer.Port)

	// Запуск сервера в отдельной горутине
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "Критическая ошибка сервера", "error", err)
		}
	}()

	// Ждем сигнал ОС или отмену контекста
	<-ctx.Done()
	logger.Log.Info("Получен сигнал завершения. Начинаем graceful shutdown...")

	//  Ограничиваем время на плавную остановку
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Пытаемся плавно остановить сервер (прекращаем прием новых запросов)
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Log.Error("Сервер завершился с ошибкой при остановке:", "error", err)
	} else {
		logger.Log.Info("Сервер успешно прекратил обработку запросов.")
	}

	// Закрываем соединения с инфраструктурой ПОСЛЕ остановки сервера
	if config.ConnServer.DB != nil {
		if err := config.ConnServer.DB.Close(); err != nil {
			logger.Log.Error("Ошибка при закрытии базы данных", "error", err)
		}
	}

	if config.ConnServer.KafkaProducer != nil {
		if err := config.ConnServer.KafkaProducer.Close(); err != nil {
			logger.Log.Error("Ошибка при закрытии Kafka Producer", "error", err)
		}
	}
	logger.Log.Info("Graceful shutdown успешно завершен.")
}
