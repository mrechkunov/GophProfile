package main

import (
	"context"
	"errors"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/handler"
	"gophprofile/internal/logger"
	"gophprofile/internal/loki"
	"gophprofile/internal/repository"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {
	config.InitServer()

	// Создаем контекст для получения системных сигналов
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	// Инициализируем Loki, передавая ctx для контроля завершения
	loki, otelShutdown := loki.InitLoggerProvider(ctx)
	defer otelShutdown()

	var router = chi.NewRouter()

	// Обертка роутера для логирования OpenTelemetry
	wrappedHandler := otelhttp.NewHandler(
		router,
		"gophprofileservice",
		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
	)

	repo := repository.NewPostgresAvatarRepository(config.ConnServer.DB)
	kafkaAdapter := &repository.KafkaProducer{
		Writer: config.ConnServer.KafkaProducer,
	}

	// Создаем хэндлер и передаем зависимости
	avatarHandler := handler.NewAvatarHandler(
		repo,
		config.ConnServer.MinioClient,
		kafkaAdapter,
		loki,
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

	loki.InfoContext(ctx, "server starting", "port", config.CfgServer.Port)

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Log.Fatalln(err.Error())
		}
	}()

	<-ctx.Done() // Ждем сигнал ОС

	loki.Info("Получен сигнал завершения. Начинаем graceful shutdown...")

	// используем переменную shutdownCtx
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Пытаемся плавно остановить сервер
	if err := server.Shutdown(shutdownCtx); err != nil {
		loki.Error("Сервер завершился с ошибкой:", "error", err)
	} else {
		loki.Info("Сервер остановлен корректно.")
	}

	// Закрываем ресурсы
	config.ConnServer.DB.Close()
	config.ConnServer.KafkaProducer.Close()

	err := logger.Log.Sync()
	if err != nil && !errors.Is(err, syscall.EBADF) && !errors.Is(err, syscall.ENOTTY) {
		fmt.Println("error while zapLogger Sync in init function", err)
	}
}
