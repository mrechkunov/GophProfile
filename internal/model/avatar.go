package model

import (
	"encoding/json"
	"errors"
	"time"
)

type Thumbnails struct {
	Small  string `json:"small,omitempty"`
	Medium string `json:"medium,omitempty"`
}

// Scan преобразует данные из БД (JSONB/text) в структуру Go
func (t *Thumbnails) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(bytes, t)
}

type Avatar struct {
	UUID             string     `db:"uuid"`
	UserID           string     `db:"user_id"`
	FileName         string     `db:"file_name"`
	MimeType         string     `db:"mime_type"`
	SizeBytes        int64      `db:"size_bytes"`
	S3Key            string     `db:"s3_key"`
	ThumbnailS3Keys  Thumbnails `db:"thumbnail_s3_keys"`
	Width            int        `db:"width"`
	Height           int        `db:"height"`
	UploadStatus     string     `db:"upload_status"`
	ProcessingStatus string     `db:"processing_status"`
	CreatedAt        time.Time  `db:"created_at"`
	UpdatedAt        time.Time  `db:"updated_at"`
	DeletedAt        time.Time  `db:"deleted_at"`
}
