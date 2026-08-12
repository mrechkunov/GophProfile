package repository

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
)

// MinioClientAPI описывает только те методы MinIO, которые нужны хэндлеру
type MinioClientAPI interface {
	PutObject(ctx context.Context, bucketName, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (info minio.UploadInfo, err error)
	RemoveObject(ctx context.Context, bucketName, objectName string, opts minio.RemoveObjectOptions) error
	GetObject(ctx context.Context, bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	BucketExists(ctx context.Context, bucketName string) (bool, error)
}
