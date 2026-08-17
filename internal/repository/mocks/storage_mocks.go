package mocks

import (
	"context"
	"gophprofile/internal/model"

	"github.com/stretchr/testify/mock"
)

type MockAvatarRepository struct {
	mock.Mock
}

func (m *MockAvatarRepository) Create(ctx context.Context, avatar *model.Avatar) error {
	args := m.Called(ctx, avatar)
	return args.Error(0)
}
func (m *MockAvatarRepository) UpdateStatus(ctx context.Context, id string, status string, thumbnailsJSON []byte) error {
	args := m.Called(ctx, id, status, thumbnailsJSON)
	return args.Error(0)
}

func (m *MockAvatarRepository) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockAvatarRepository) GetByID(ctx context.Context, avatarID string) (*model.Avatar, error) {
	args := m.Called(ctx, avatarID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Avatar), args.Error(1)
}

func (m *MockAvatarRepository) GetByUserID(ctx context.Context, userID string) (*model.Avatar, error) {
	args := m.Called(ctx, userID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Avatar), args.Error(1)
}

func (m *MockAvatarRepository) SoftDelete(ctx context.Context, id string) (*model.Avatar, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*model.Avatar), args.Error(1)
}
