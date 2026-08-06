// internal/model/avatar.go
package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

// Thumbnails тип для работы с JSONB в PostgreSQL
type Thumbnails map[string]string

func (t Thumbnails) Value() (driver.Value, error) {
	return json.Marshal(t)
}

func (t *Thumbnails) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}
	return json.Unmarshal(b, &t)
}

type Avatar struct {
	UUID              string     `db:"uuid"`
	UserID            string     `db:"user_id"`
	FileName          string     `db:"file_name"`
	MimeType          string     `db:"mime_type"`
	SizeBytes         int64      `db:"size_bytes"`
	S3Key             string     `db:"s3_key"`
	Thumbnail_S3_Keys Thumbnails `db:"thumbnail_s3_keys"`
	UploadStatus      string     `db:"upload_status"`
	ProcessingStatus  string     `db:"processing_status"`
	CreatedAt         time.Time  `db:"created_at"`
	UpdatedAt         time.Time  `db:"updated_at"`
	DeletedAt         time.Time  `db:"deleted_at"`
}
