package main

import (
	"context"
	"errors"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/logger"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
)

func main() {
	config.Init()
	// Создаем контекст для получения системных сигналов
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	defer stop()

	var router = chi.NewRouter()
	router.Get("/", logger.WithLogging(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("test"))
	}))

	var server = &http.Server{
		Addr:    config.ConfigAddresses.ServerBindAddress,
		Handler: router,
	}

	logger.Log.Infoln("server starting:", config.ConfigAddresses.ServerBindAddress, "http")
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
	config.DBconn.Close()
	err := logger.Log.Sync()
	if err != nil && !errors.Is(err, syscall.EBADF) && !errors.Is(err, syscall.ENOTTY) {
		fmt.Println("error while zapLogger Sync in init function", err)
	}
}
