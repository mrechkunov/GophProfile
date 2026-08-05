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
	ID         string     `db:"id"`
	UserID     string     `db:"user_id"`
	OriginURL  string     `db:"origin_url"`
	Thumbnails Thumbnails `db:"thumbnails"`
	Status     string     `db:"status"`
	CreatedAt  time.Time  `db:"created_at"`
	UpdatedAt  time.Time  `db:"updated_at"`
}
