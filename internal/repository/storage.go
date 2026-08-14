package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"gophprofile/internal/config"
	"gophprofile/internal/model"
	"time"
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

var ErrAvatarNotFound = errors.New("Avatar not found")

// Create создает первичную запись со статусом processing
func (r *PostgresAvatarRepository) Create(ctx context.Context, avatar *model.Avatar) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	sqlStatement := `
		INSERT INTO avatars (uuid, user_id, file_name, mime_type, size_bytes, s3_key, upload_status, processing_status, width, height)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`
	_, err := r.db.ExecContext(ctxWithTimeout, sqlStatement,
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

	_, err := config.ConnWorker.DB.ExecContext(ctx, sqlStatement, thumbnailsJSON, status, id)
	if err != nil {
		return fmt.Errorf("failed to update avatar row in database: %w", err)
	}
	return nil
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
	err := r.db.QueryRowContext(ctx, sqlStatement, id).Scan(
		&avatar.UUID,
		&avatar.UserID,
		&avatar.S3Key,
		&avatar.ThumbnailS3Keys,
	)

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
	err := r.db.QueryRowContext(ctx, sqlStatement, avatarID).Scan(
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
	err := r.db.QueryRowContext(ctx, sqlStatement, userID).Scan(
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

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrAvatarNotFound
		}
		return nil, err
	}

	return &avatar, nil
}

func (r *PostgresAvatarRepository) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}
