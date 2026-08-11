package main

import (
	"context"
	"errors"
	"gophprofile/internal/config"
	"gophprofile/internal/handler"
	"gophprofile/internal/logger"
	_ "gophprofile/internal/logger"
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

	// Создаем контекст для получения системных сигналов
	ctxSig, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()
	ctx := context.Background()
	logger.Log, logger.OtelShutdown = logger.InitLoggerProvider(ctx)
	config.InitServer(ctx)
	// Инициализация логирования с slog
	//Log, otelShutdown := logger.InitLoggerProvider(ctx)

	var router = chi.NewRouter()

	repo := repository.NewPostgresAvatarRepository(config.ConnServer.DB)
	kafkaAdapter := &repository.KafkaProducer{
		Writer: config.ConnServer.KafkaProducer,
	}

	// Используем otelhttp middleware для автоматического логирования HTTP запросов
	// otelhttp автоматически логирует:
	// - HTTP метод, путь, статус код
	// - Длительность запроса
	// - User-Agent, Remote Addr и другие атрибуты
	wrappedHandler := otelhttp.NewHandler(
		router,
		"gophprofileservice",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)
	// Создаем хэндлер и передаем зависимости
	avatarHandler := handler.NewAvatarHandler(
		repo,
		config.ConnServer.MinioClient,
		kafkaAdapter,
		logger.Log,
	)

	// Маршруты к хендлерам
	router.Get("/", avatarHandler.IndexHandler)
	router.Post("/api/v1/avatars", avatarHandler.PostUploadAvatarHandler)
	router.Get("/api/v1/avatars/{avatar_id}", avatarHandler.GetAvatarHandler)
	router.Get("/api/v1/users/{user_id}/avatar", avatarHandler.GetUserAvatarHandler)
	router.Get("/api/v1/avatars/{avatar_id}/metadata", avatarHandler.GetAvatarMetadataHandler)
	router.Delete("/api/v1/avatars/{avatar_id}", avatarHandler.DeleteAvatarHandler)
	router.Get("/health", avatarHandler.HealthCheckHandler)

	var server = &http.Server{
		Addr:    config.CfgServer.Port,
		Handler: wrappedHandler,
	}
	logger.Log.Info("server starting", "port", config.CfgServer.Port)

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, err.Error())
		}
	}()

	<-ctxSig.Done() // Ждем сигнал ОС

	logger.Log.Info("Получен сигнал завершения. Начинаем graceful shutdown...")

	// используем переменную shutdownCtx
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Пытаемся плавно остановить сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Log.Error("Сервер завершился с ошибкой:", "error", err)
	} else {
		logger.Log.Info("Сервер остановлен корректно.")
	}

	// Закрываем ресурсы
	config.ConnServer.DB.Close()
	config.ConnServer.KafkaProducer.Close()

	logger.OtelShutdown() // гасим логирование
}
