package model

type AvatarUploadEvent struct {
	AvatarID string `json:"avatar_id"`
	UserID   string `json:"user_id"`
	S3Key    string `json:"s3_key"`
}

type AvatarProcessEvent struct {
	AvatarID string `json:"avatar_id"`
	//Operations []ProcessingOp `json:"operations"`
}

type AvatarDeleteEvent struct {
	AvatarID string   `json:"avatar_id"`
	S3Keys   []string `json:"s3_keys"`
}

type AvatarResizeTask struct {
	AvatarID   string `json:"avatar_id"`
	UserID     string `json:"user_id"`
	BucketName string `json:"bucket_name"`
	ObjectKey  string `json:"object_key"`
	Sizes      []int  `json:"sizes"` // [100, 300]
}
