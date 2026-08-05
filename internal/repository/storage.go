package repository

import "database/sql"

type StorageAvatars struct {
	DBconnection *sql.DB
}

// создаем новый сторадж для работы с таблицей аватаров
func NewStorageAvatars(dbConn *sql.DB) StorageAvatars {
	return StorageAvatars{DBconnection: dbConn}
}

// select
// select all by UUID
// insert
// delete (set deleted)
//
