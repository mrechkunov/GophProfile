-- migrations/000001_create_users_table.up.sql
-- Создание таблицы пользователей
CREATE TABLE IF NOT EXISTS users (
    u_login VARCHAR(255) PRIMARY KEY,
    u_password VARCHAR(255) NOT NULL,
    u_bearer VARCHAR(255) NOT NULL
);

-- Базовый индекс для поиска по логину
CREATE INDEX idx_login ON users(u_login);