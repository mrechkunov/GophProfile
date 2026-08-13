package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ComponentStatus struct {
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	LatencyMs int64  `json:"latency_ms"`
}

type HealthResponse struct {
	Status     string                     `json:"status"`
	Components map[string]ComponentStatus `json:"components"`
}

// GET /health
func (h *AvatarHandler) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Ограничиваем проверку по времени. Используем r.Context(), чтобы не плодить оторванные контексты
	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second)
	defer cancel()

	components := make(map[string]ComponentStatus)
	isSystemUp := true

	// Проверка PostgreSQL
	startDB := time.Now()
	errDB := h.repo.Ping(ctx)
	latencyDB := time.Since(startDB).Milliseconds()

	dbStatus := "UP"
	if errDB != nil {
		isSystemUp = false
		dbStatus = "DOWN"
		components["database"] = ComponentStatus{Status: dbStatus, Error: errDB.Error(), LatencyMs: latencyDB}
	} else {
		components["database"] = ComponentStatus{Status: dbStatus, LatencyMs: latencyDB}
	}
	// Фиксируем задержку в Prometheus
	h.metrics.HealthLatencyDB.Record(ctx, float64(latencyDB), metric.WithAttributes(
		attribute.String("component_status", dbStatus),
	))

	// Проверка MinIO S3
	startS3 := time.Now()
	exists, errS3 := h.s3.BucketExists(ctx, BucketName)
	latencyS3 := time.Since(startS3).Milliseconds()

	s3Status := "UP"
	if errS3 != nil || !exists {
		isSystemUp = false
		s3Status = "DOWN"
		errText := "bucket does not exist"
		if errS3 != nil {
			errText = errS3.Error()
		}
		components["storage"] = ComponentStatus{Status: s3Status, Error: errText, LatencyMs: latencyS3}
	} else {
		components["storage"] = ComponentStatus{Status: s3Status, LatencyMs: latencyS3}
	}
	// Фиксируем задержку в Prometheus
	h.metrics.HealthLatencyS3.Record(ctx, float64(latencyS3), metric.WithAttributes(
		attribute.String("component_status", s3Status),
	))

	// Проверка Kafka Брокера
	startKafka := time.Now()
	errKafka := h.kafka.Ping(ctx)
	latencyKafka := time.Since(startKafka).Milliseconds()

	kafkaStatus := "UP"
	if errKafka != nil {
		isSystemUp = false
		kafkaStatus = "DOWN"
		components["broker"] = ComponentStatus{Status: kafkaStatus, Error: errKafka.Error(), LatencyMs: latencyKafka}
	} else {
		components["broker"] = ComponentStatus{Status: kafkaStatus, LatencyMs: latencyKafka}
	}
	// Фиксируем задержку в Prometheus
	h.metrics.HealthLatencyKafka.Record(ctx, float64(latencyKafka), metric.WithAttributes(
		attribute.String("component_status", kafkaStatus),
	))

	// Формируем финальный статус системы
	globalStatus := "UP"
	if !isSystemUp {
		globalStatus = "DOWN"
		w.WriteHeader(http.StatusServiceUnavailable)
	} else {
		w.WriteHeader(http.StatusOK)
	}

	json.NewEncoder(w).Encode(HealthResponse{
		Status:     globalStatus,
		Components: components,
	})

	// Логируем через ctx, содержащий таймаут, чтобы логгер видел актуальное состояние горутины
	h.logger.DebugContext(ctx, "Health check completed", "global_status", globalStatus)
}
