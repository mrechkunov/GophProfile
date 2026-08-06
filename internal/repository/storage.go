// internal/repository/avatar.go
package repository

import (
	"context"
	"database/sql"
	"fmt"
	"gophprofile/internal/config"
	"gophprofile/internal/model"
	"time"
)

type AvatarRepository interface {
	Create(ctx context.Context, avatar *model.Avatar) error
	UpdateStatus(ctx context.Context, id string, status string, thumbnails model.Thumbnails) error
	GetByUserID(ctx context.Context, userID string) (*model.Avatar, error)
	//delete
}

type PostgresAvatarRepository struct {
	db *sql.DB
}

func NewPostgresAvatarRepository(db *sql.DB) *PostgresAvatarRepository {
	return &PostgresAvatarRepository{db: db}
}

// Create создает первичную запись со статусом processing
func (r *PostgresAvatarRepository) Create(ctx context.Context, avatar *model.Avatar) error {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()
	sqlStatement := `
		INSERT INTO avatars (uuid, user_id, file_name, mime_type, size_bytes, s3_key, upload_status, processing_status)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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

// // GetByUserID находит последний активный аватар пользователя
// func (r *PostgresAvatarRepository) GetByUserID(ctx context.Context, userID string) (*model.Avatar, error) {
// 	query := `
// 		SELECT id, user_id, origin_url, thumbnails, status, created_at, updated_at
// 		FROM avatars
// 		WHERE user_id = $1
// 		ORDER BY created_at DESC
// 		LIMIT 1
// 	`
// 	var avatar model.Avatar
// 	err := r.db.QueryRowContext(ctx, query, userID).Scan(
// 		&avatar.UUID,
// 		&avatar.UserID,
// 		&avatar.OriginURL,
// 		&avatar.Thumbnails,
// 		&avatar.,
// 		&avatar.CreatedAt,
// 		&avatar.UpdatedAt,
// 	)
// 	if err == sql.ErrNoRows {
// 		return nil, nil // Аватар не найден
// 	}
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &avatar, nil
// }
