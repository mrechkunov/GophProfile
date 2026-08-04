package handler_test

import (
	"gophprofile/internal/handler"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestIndexHandler_Success(t *testing.T) {
	// Создаем временную директорию и тестовый HTML-файл
	tmpDir := "./web/static"
	err := os.MkdirAll(tmpDir, 0755)
	if err != nil {
		t.Fatalf("Не удалось создать директорию для теста: %v", err)
	}
	// Удаляем созданную структуру после завершения теста
	defer os.RemoveAll("./web")

	testHTML := "<html><body>Hello World</body></html>"
	filePath := filepath.Join(tmpDir, "index.html")
	err = os.WriteFile(filePath, []byte(testHTML), 0644)
	if err != nil {
		t.Fatalf("Не удалось создать тестовый файл: %v", err)
	}

	// 1. Создаем тестовый HTTP-запрос (метод GET, эндпоинт "/")
	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("Не удалось создать запрос: %v", err)
	}

	// 2. Создаем ResponseRecorder для записи ответа сервера
	rr := httptest.NewRecorder()

	// 3. Вызываем тестируемый обработчик напрямую
	handler := http.HandlerFunc(handler.IndexHandler)
	handler.ServeHTTP(rr, req)

	// 4. Проверяем HTTP-статус ответа (должен быть 200 OK)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("Обработчик вернул неверный статус: получили %v, ожидали %v", status, http.StatusOK)
	}

	// 5. Проверяем содержимое ответа
	expectedContentType := "text/html; charset=utf-8"
	if contentType := rr.Header().Get("Content-Type"); contentType != expectedContentType {
		t.Errorf("Неверный Content-Type: получили %v, ожидали %v", contentType, expectedContentType)
	}

	if rr.Body.String() != testHTML {
		t.Errorf("Неверное тело ответа: получили %v, ожидали %v", rr.Body.String(), testHTML)
	}
}

func TestIndexHandler_FileNotFound(t *testing.T) {
	// Убедимся, что файла точно нет (удаляем временную папку, если она осталась)
	os.RemoveAll("./web")

	req, err := http.NewRequest("GET", "/", nil)
	if err != nil {
		t.Fatalf("Не удалось создать запрос: %v", err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(handler.IndexHandler)
	handler.ServeHTTP(rr, req)

	// Если файла нет, http.ServeFile должен вернуть статус 404 Not Found
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("Обработчик вернул неверный статус при отсутствии файла: получили %v, ожидали %v", status, http.StatusNotFound)
	}
}
