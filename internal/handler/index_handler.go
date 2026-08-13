package handler

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// GET /
func (h *AvatarHandler) IndexHandler(w http.ResponseWriter, r *http.Request) {
	// Стартуем спан для отслеживания рендеринга страницы
	ctx, span := tracer.Start(r.Context(), "IndexHandler",
		trace.WithSpanKind(trace.SpanKindServer),
	)
	defer span.End()

	staticFilePath := "./web/static/index.html"
	span.SetAttributes(attribute.String("file.path", staticFilePath))

	// Отдаем статический файл
	// Оборачиваем в под-спан для замера скорости работы дисковой подсистемы
	err := func() error {
		_, diskSpan := tracer.Start(ctx, "OS:ServeStaticFile", trace.WithSpanKind(trace.SpanKindClient))
		defer diskSpan.End()

		http.ServeFile(w, r, staticFilePath)
		return nil
	}()

	if err != nil {
		// На случай форс-мажора (хоть ServeFile сам пишет заголовки, зафиксируем сбой в телеметрии)
		h.metrics.IndexViewsCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "error"),
		))
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to serve index.html")
		return
	}

	// Записываем успешную метрику
	h.metrics.IndexViewsCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", "success"),
	))

	// Логируем событие с контекстом трейсинга
	h.logger.InfoContext(ctx, "Index page served successfully")
	span.SetStatus(codes.Ok, "Success")
}
