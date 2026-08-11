package loki

// package loki

// import (
// 	"context"
// 	"log"
// 	"log/slog"
// 	"time"

// 	"go.opentelemetry.io/contrib/bridges/otelslog"
// 	"go.opentelemetry.io/otel"
// 	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
// 	sdklog "go.opentelemetry.io/otel/sdk/log"
// 	"go.opentelemetry.io/otel/sdk/resource"
// 	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
// )

// func InitLoggerProvider(ctx context.Context) (*slog.Logger, func()) {
// 	// Создаём gRPC Exporter для логов (порт 4317)
// 	exporter, err := otlploggrpc.New(ctx)
// 	if err != nil {
// 		log.Fatalf("failed to create OTLP log exporter: %v", err)
// 	}

// 	// Метаинформация (Resource)
// 	res, err := resource.New(ctx,
// 		resource.WithFromEnv(),
// 		resource.WithTelemetrySDK(),
// 		resource.WithAttributes(
// 			semconv.ServiceNameKey.String("gophprofileservice"),
// 			semconv.ServiceVersionKey.String("1.0.0"),
// 		),
// 	)
// 	if err != nil {
// 		log.Fatalf("failed to create resource: %v", err)
// 	}

// 	// Инициализируем LoggerProvider
// 	loggerProvider := sdklog.NewLoggerProvider(
// 		sdklog.WithResource(res),
// 		sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
// 	)

// 	// Создаем slog Handler через otelslog bridge
// 	handler := otelslog.NewHandler(
// 		"gophprofileservice",
// 		otelslog.WithLoggerProvider(loggerProvider),
// 	)

// 	// Создаем slog логгер с этим handler'ом
// 	logger := slog.New(handler)

// 	// Устанавливаем как глобальный логгер
// 	slog.SetDefault(logger)

// 	// Возвращаем функцию для корректного завершения (flush данных перед выходом)
// 	shutdown := func() {
// 		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 		defer cancel()
// 		if err := loggerProvider.Shutdown(ctx); err != nil {
// 			otel.Handle(err)
// 		}
// 	}

// 	return logger, shutdown
// }

// func main() {
// 	ctx := context.Background()

// 	// Инициализация логирования с slog
// 	loki, otelShutdown := InitLoggerProvider(ctx)
// 	defer otelShutdown()

// 	mux := http.NewServeMux()
// 	handler := NewAppHandler(loki)

// 	// Регистрируем handlers
// 	mux.Handle("GET /", http.HandlerFunc(handler.Index))
// 	mux.Handle("GET /health", http.HandlerFunc(handleHealth))

// Используем otelhttp middleware для автоматического логирования HTTP запросов
// otelhttp автоматически логирует:
// - HTTP метод, путь, статус код
// - Длительность запроса
// - User-Agent, Remote Addr и другие атрибуты
// 	wrappedHandler := otelhttp.NewHandler(
// 		mux,
// 		"gophprofileservice",
// 		otelhttp.WithMessageEvents(otelhttp.ReadEvents, otelhttp.WriteEvents),
// 	)

// 	loki.Info("Server starting", "port", 8080)
// 	log.Fatal(http.ListenAndServe(":8080", wrappedHandler))
// }

// type AppHandler struct {
// 	logger *slog.Logger
// }

// func NewAppHandler(logger *slog.Logger) *AppHandler {
// 	return &AppHandler{
// 		logger: logger.With("component", "AppHandler"),
// 	}
// }

// func (h *AppHandler) Index(w http.ResponseWriter, r *http.Request) {
// 	// Логируем начало обработки
// 	h.logger.InfoContext(r.Context(), "Processing index request")

// 	// Логируем вложенную операцию
// 	h.logger.DebugContext(r.Context(), "Simulating workload",
// 		"custom.user_id", "12345",
// 		"operation", "simulate_workload",
// 	)

// 	h.logger.InfoContext(r.Context(), "Index request processing completed")

// 	fmt.Fprintf(w, "Hello, World!\n")
// }

// func handleHealth(w http.ResponseWriter, r *http.Request) {
// 	slog.DebugContext(r.Context(), "Health check")
// 	fmt.Fprintf(w, "OK\n")
// }
