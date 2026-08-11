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
	// инициализируем Loki
	loki, otelShutdown := loki.InitLoggerProvider(context.Background())
	defer otelShutdown()
	var router = chi.NewRouter()
	// обертка хендлера для логирования
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
	router.Get("/", handler.IndexHandler)
	router.Post("/api/v1/avatars", avatarHandler.PostUploadAvatarHandler)
	// Получение
	router.Get("/api/v1/avatars/{avatar_id}", avatarHandler.GetAvatarHandler)
	router.Get("/api/v1/users/{user_id}/avatar", avatarHandler.GetUserAvatarHandler)
	router.Get("/api/v1/avatars/{avatar_id}/metadata", avatarHandler.GetAvatarMetadataHandler)
	// Удаление
	router.Delete("/api/v1/avatars/{avatar_id}", avatarHandler.DeleteAvatarHandler)
	// healthCheck
	router.Get("/health", avatarHandler.HealthCheckHandler)
	var server = &http.Server{
		Addr:    config.CfgServer.Port,
		Handler: wrappedHandler,
	}

	loki.Info("server starting:", config.CfgServer.Port, "http")
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Log.Fatalln(err.Error())
		}
	}()
	<-ctx.Done()
	logger.Log.Infoln("Получен сигнал завершения. Начинаем graceful shutdown...")
	// Создаем контекст с таймаутом для завершения активных запросов
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Пытаемся плавно остановить сервер
	if err := server.Shutdown(ctx); err != nil {
		logger.Log.Infoln("Сервер завершился с ошибкой:", err)
	} else {
		logger.Log.Infoln("Сервер остановлен корректно.")
	}

	config.ConnServer.DB.Close()
	config.ConnServer.KafkaProducer.Close()
	err := logger.Log.Sync()
	if err != nil && !errors.Is(err, syscall.EBADF) && !errors.Is(err, syscall.ENOTTY) {
		fmt.Println("error while zapLogger Sync in init function", err)
	}
}
