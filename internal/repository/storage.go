// internal/repository/avatar.go
package repository

import (
	"context"
	"database/sql"
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
	query := `
		INSERT INTO avatars (id, user_id, origin_url, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, err := r.db.ExecContext(ctx, query,
		avatar.ID,
		avatar.UserID,
		avatar.OriginURL,
		avatar.Status,
		avatar.CreatedAt,
		avatar.UpdatedAt,
	)
	return err
}

// UpdateStatus вызывается воркером после успешного или неуспешного ресайза
func (r *PostgresAvatarRepository) UpdateStatus(ctx context.Context, id string, status string, thumbnails model.Thumbnails) error {
	query := `
		UPDATE avatars 
		SET status = $1, thumbnails = $2, updated_at = $3 
		WHERE id = $4
	`
	_, err := r.db.ExecContext(ctx, query, status, thumbnails, time.Now().UTC(), id)
	return err
}

// GetByUserID находит последний активный аватар пользователя
func (r *PostgresAvatarRepository) GetByUserID(ctx context.Context, userID string) (*model.Avatar, error) {
	query := `
		SELECT id, user_id, origin_url, thumbnails, status, created_at, updated_at 
		FROM avatars 
		WHERE user_id = $1 
		ORDER BY created_at DESC 
		LIMIT 1
	`
	var avatar model.Avatar
	err := r.db.QueryRowContext(ctx, query, userID).Scan(
		&avatar.ID,
		&avatar.UserID,
		&avatar.OriginURL,
		&avatar.Thumbnails,
		&avatar.Status,
		&avatar.CreatedAt,
		&avatar.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // Аватар не найден
	}
	if err != nil {
		return nil, err
	}
	return &avatar, nil
}
