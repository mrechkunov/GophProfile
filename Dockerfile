# --- Этап 1: Сборка приложения ---
FROM golang:1.26.5-trixie AS builder

WORKDIR /app

# Копируем файлы зависимостей

COPY go.mod go.sum ./
RUN go mod download


# Копируем исходный код
COPY . .

# Компилируем бинарник (CGO отключен для статической линковки)
RUN CGO_ENABLED=0 GOOS=linux go build -o gophprofile ./cmd/server/main.go

# --- Этап 2: Финальный образ ---
FROM alpine

WORKDIR /root/

# Копируем скомпилированный файл из предыдущего шага
COPY /migrations/* ./migrations/
COPY --from=builder /app/gophprofile .

# Указываем порт, который слушает приложение
EXPOSE 8080

# Запуск приложения
CMD ["./gophprofile"]
