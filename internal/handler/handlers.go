package handler

import (
	"gophprofile/internal/repository"
	"log/slog"
	"time"
)

const MaxFileSize = 10 * 1024 * 1024
const BucketName = "avatars"

type AvatarResponse struct {
	ID        string    `json:"id"`
	UserID    string    `json:"user_id"`
	URL       string    `json:"url"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type ErrorResponse struct {
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

type SizeErrorResponse struct {
	Error   string `json:"error"`
	MaxSize int64  `json:"max_size"`
}

// AvatarHandler объединяет зависимости для работы с аватарами
type AvatarHandler struct {
	repo   repository.AvatarRepository
	s3     repository.MinioClientAPI
	kafka  repository.KafkaProducerAPI
	logger *slog.Logger
}

// NewAvatarHandler — конструктор хэндлера
func NewAvatarHandler(repo repository.AvatarRepository, s3 repository.MinioClientAPI, kafka repository.KafkaProducerAPI, loki *slog.Logger) *AvatarHandler {
	return &AvatarHandler{
		repo:   repo,
		s3:     s3,
		kafka:  kafka,
		logger: loki.With("component", "AvatarHandler"),
	}
}

type Dimensions struct {
	Width  int `json:"width"`
	Height int `json:"height"`
}

type ThumbnailInfo struct {
	Size string `json:"size"`
	URL  string `json:"url"`
}

type AvatarMetadataResponse struct {
	ID         string          `json:"id"`
	UserID     string          `json:"user_id"`
	FileName   string          `json:"file_name"`
	MimeType   string          `json:"mime_type"`
	Size       int64           `json:"size"`
	Dimensions Dimensions      `json:"dimensions"`
	Thumbnails []ThumbnailInfo `json:"thumbnails"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}
