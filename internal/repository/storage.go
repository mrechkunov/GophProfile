package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"gophprofile/internal/model"
)

type AvatarRepository interface {
	Create(ctx context.Context, avatar *model.Avatar) error
	UpdateStatus(ctx context.Context, id string, status string, thumbnails []byte) error
	SoftDelete(ctx context.Context, id string) (*model.Avatar, error)
	GetByID(ctx context.Context, avatarID string) (*model.Avatar, error)
	GetByUserID(ctx context.Context, userID string) (*model.Avatar, error)
	Ping(ctx context.Context) error
}

type PostgresAvatarRepository struct {
	db *sql.DB
}

func NewPostgresAvatarRepository(db *sql.DB) *PostgresAvatarRepository {
	return &PostgresAvatarRepository{db: db}
}

var ErrAvatarNotFound = errors.New("avatar not found")

// Лимиты времени на выполнение операций (Resource Limits / Timeouts)
const (
	writeTimeout = 5 * time.Second
	readTimeout  = 3 * time.Second
	maxRetries   = 6
)

// Create создает первичную запись со статусом processing
func (r *PostgresAvatarRepository) Create(ctx context.Context, avatar *model.Avatar) error {
	sqlStatement := `
		INSERT INTO avatars (uuid, user_id, file_name, mime_type, size_bytes, s3_key, upload_status, processing_status, width, height)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	return r.executeWithRetry(ctx, writeTimeout, func(reqCtx context.Context) error {
		_, err := r.db.ExecContext(reqCtx, sqlStatement,
			avatar.UUID,
			avatar.UserID,
			avatar.FileName,
			avatar.MimeType,
			avatar.SizeBytes,
			avatar.S3Key,
			avatar.UploadStatus,
			avatar.ProcessingStatus,
			avatar.Width,
			avatar.Height,
		)
		return err
	})
}

// UpdateStatus вызывается воркером после успешного или неуспешного ресайза
func (r *PostgresAvatarRepository) UpdateStatus(ctx context.Context, id string, status string, thumbnailsJSON []byte) error {
	sqlStatement := `
		UPDATE avatars 
		SET thumbnail_s3_keys = $1, 
		    upload_status = $2, 
		    processing_status = 'completed', 
		    updated_at = NOW()
		WHERE uuid = $3`

	return r.executeWithRetry(ctx, writeTimeout, func(reqCtx context.Context) error {
		_, err := r.db.ExecContext(reqCtx, sqlStatement, thumbnailsJSON, status, id)
		if err != nil {
			return fmt.Errorf("failed to update avatar row: %w", err)
		}
		return nil
	})
}

// SoftDelete помечает аватар удаленным и возвращает его ключи S3 для последующей очистки
func (r *PostgresAvatarRepository) SoftDelete(ctx context.Context, id string) (*model.Avatar, error) {
	sqlStatement := `
		UPDATE avatars 
		SET deleted_at = NOW(), updated_at = NOW() 
		WHERE uuid = $1 AND deleted_at IS NULL
		RETURNING uuid, user_id, s3_key, thumbnail_s3_keys
	`

	var avatar model.Avatar
	err := r.executeWithRetry(ctx, writeTimeout, func(reqCtx context.Context) error {
		return r.db.QueryRowContext(reqCtx, sqlStatement, id).Scan(
			&avatar.UUID,
			&avatar.UserID,
			&avatar.S3Key,
			&avatar.ThumbnailS3Keys,
		)
	})

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAvatarNotFound
		}
		return nil, err
	}

	return &avatar, nil
}

// GetByID находит аватарку по её уникальному UUID
func (r *PostgresAvatarRepository) GetByID(ctx context.Context, avatarID string) (*model.Avatar, error) {
	sqlStatement := `
		SELECT uuid, user_id, file_name, mime_type, size_bytes, s3_key, 
		       thumbnail_s3_keys, upload_status, processing_status, 
		       created_at, updated_at, width, height
		FROM avatars
		WHERE uuid = $1 AND deleted_at IS NULL
	`

	var avatar model.Avatar
	err := r.executeWithRetry(ctx, readTimeout, func(reqCtx context.Context) error {
		return r.db.QueryRowContext(reqCtx, sqlStatement, avatarID).Scan(
			&avatar.UUID,
			&avatar.UserID,
			&avatar.FileName,
			&avatar.MimeType,
			&avatar.SizeBytes,
			&avatar.S3Key,
			&avatar.ThumbnailS3Keys,
			&avatar.UploadStatus,
			&avatar.ProcessingStatus,
			&avatar.CreatedAt,
			&avatar.UpdatedAt,
			&avatar.Width,
			&avatar.Height,
		)
	})

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAvatarNotFound
		}
		return nil, err
	}

	return &avatar, nil
}

// GetByUserID находит актуальную аватарку конкретного пользователя
func (r *PostgresAvatarRepository) GetByUserID(ctx context.Context, userID string) (*model.Avatar, error) {
	sqlStatement := `
		SELECT uuid, user_id, file_name, mime_type, size_bytes, s3_key, 
		       thumbnail_s3_keys, upload_status, processing_status, 
		       created_at, updated_at, width, height
		FROM avatars
		WHERE user_id = $1 AND deleted_at IS NULL
		ORDER BY created_at DESC
		LIMIT 1
	`

	var avatar model.Avatar
	err := r.executeWithRetry(ctx, readTimeout, func(reqCtx context.Context) error {
		return r.db.QueryRowContext(reqCtx, sqlStatement, userID).Scan(
			&avatar.UUID,
			&avatar.UserID,
			&avatar.FileName,
			&avatar.MimeType,
			&avatar.SizeBytes,
			&avatar.S3Key,
			&avatar.ThumbnailS3Keys,
			&avatar.UploadStatus,
			&avatar.ProcessingStatus,
			&avatar.CreatedAt,
			&avatar.UpdatedAt,
			&avatar.Width,
			&avatar.Height,
		)
	})

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAvatarNotFound
		}
		return nil, err
	}

	return &avatar, nil
}

// Ping проверяет доступность СУБД с коротким таймаутом
func (r *PostgresAvatarRepository) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	return r.db.PingContext(pingCtx)
}

// ==========================================
// ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ СТАБИЛЬНОСТИ
// ==========================================

// executeWithRetry инкапсулирует логику таймаутов на запрос и экспоненциального бэкоффа для ретраев
func (r *PostgresAvatarRepository) executeWithRetry(ctx context.Context, timeout time.Duration, operation func(reqCtx context.Context) error) error {
	var err error
	backoff := 200 * time.Millisecond

	for attempt := 1; attempt <= maxRetries; attempt++ {
		reqCtx, cancel := context.WithTimeout(ctx, timeout)
		err = operation(reqCtx)
		cancel()

		if err == nil {
			return nil // Успешно выполнено
		}

		// Если ошибка фатальная (сбой логики/валидации) или контекст верхнего уровня отменен — ретраить нельзя
		if errors.Is(err, sql.ErrNoRows) || errors.Is(ctx.Err(), context.Canceled) || r.isFatalDBError(err) {
			return err
		}

		// Пауза перед следующей попыткой (только при сетевых морганиях или дедлоках блокировок)
		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff *= 2 // Экспоненциальный рост ожидания: 200мс, 400мс, 800мс
			}
		}
	}
	return fmt.Errorf("operation failed after %d attempts: %w", maxRetries, err)
}

// isFatalDBError отсекает ошибки синтаксиса или ограничений БД, которые ретраить бессмысленно
func (r *PostgresAvatarRepository) isFatalDBError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// Ошибки плохих запросов, дубликатов индексов или отсутствия таблиц
	return strings.Contains(errStr, "syntax error") ||
		strings.Contains(errStr, "duplicate key") ||
		strings.Contains(errStr, "does not exist")
}
