package handler_test

import (
	"gophprofile/internal/handler"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
)

func TestIndexHandler_Success(t *testing.T) {
	// Изолируем otel
	mp := metric.NewMeterProvider()
	otel.SetMeterProvider(mp)

	metrics, err := handler.NewAvatarMetrics()
	assert.NoError(t, err)

	discardLogger := slog.New(slog.DiscardHandler)

	h := handler.NewAvatarHandler(nil, nil, nil, discardLogger, metrics)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rr := httptest.NewRecorder()

	h.IndexHandler(rr, req)

	// Проверка
	assert.NotPanics(t, func() {
		h.IndexHandler(rr, req)
	})
}
