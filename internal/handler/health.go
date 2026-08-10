package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type ComponentStatus struct {
	Status    string `json:"status"` // "UP" или "DOWN"
	Error     string `json:"error,omitempty"`
	LatencyMs int64  `json:"latency_ms"`
}

type HealthResponse struct {
	Status     string                     `json:"status"` // "UP" или "DOWN"
	Components map[string]ComponentStatus `json:"components"`
}

// GET /health
func (h *AvatarHandler) HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	ctx, cancel := context.WithTimeout(r.Context(), 4*time.Second) // Ограничиваем проверку по времени
	defer cancel()

	components := make(map[string]ComponentStatus)
	isSystemUp := true

	// Проверка PostgreSQL
	startDB := time.Now()
	errDB := h.repo.Ping(ctx)
	latencyDB := time.Since(startDB).Milliseconds()
	if errDB != nil {
		isSystemUp = false
		components["database"] = ComponentStatus{Status: "DOWN", Error: errDB.Error(), LatencyMs: latencyDB}
	} else {
		components["database"] = ComponentStatus{Status: "UP", LatencyMs: latencyDB}
	}

	// Проверка MinIO S3
	startS3 := time.Now()
	exists, errS3 := h.s3.BucketExists(ctx, BucketName)
	latencyS3 := time.Since(startS3).Milliseconds()
	if errS3 != nil || !exists {
		isSystemUp = false
		errText := "bucket does not exist"
		if errS3 != nil {
			errText = errS3.Error()
		}
		components["storage"] = ComponentStatus{Status: "DOWN", Error: errText, LatencyMs: latencyS3}
	} else {
		components["storage"] = ComponentStatus{Status: "UP", LatencyMs: latencyS3}
	}

	// Проверка Kafka Брокера
	startKafka := time.Now()
	errKafka := h.kafka.Ping(ctx)
	latencyKafka := time.Since(startKafka).Milliseconds()

	if errKafka != nil {
		isSystemUp = false
		components["broker"] = ComponentStatus{Status: "DOWN", Error: errKafka.Error(), LatencyMs: latencyKafka}
	} else {
		components["broker"] = ComponentStatus{Status: "UP", LatencyMs: latencyKafka}
	}

	// Формируем финальный статус системы
	globalStatus := "UP"
	if !isSystemUp {
		globalStatus = "DOWN"
		w.WriteHeader(http.StatusServiceUnavailable) // 503 если что-то упало
	} else {
		w.WriteHeader(http.StatusOK) // 200 если всё отлично
	}

	json.NewEncoder(w).Encode(HealthResponse{
		Status:     globalStatus,
		Components: components,
	})
}
