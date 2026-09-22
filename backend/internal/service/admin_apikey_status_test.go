//go:build unit

package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdminService_AdminSetAPIKeyStatus_Disable(t *testing.T) {
	existing := &APIKey{ID: 7, Key: "sk-cc", Status: StatusAPIKeyActive}
	repo := &apiKeyRepoStubForGroupUpdate{key: existing}
	cache := &authCacheInvalidatorStub{}
	svc := &adminServiceImpl{apiKeyRepo: repo, authCacheInvalidator: cache}

	got, err := svc.AdminSetAPIKeyStatus(context.Background(), 7, StatusAPIKeyInactive)
	require.NoError(t, err)
	require.Equal(t, StatusAPIKeyInactive, got.Status)
	require.NotNil(t, repo.updated)
	require.Equal(t, StatusAPIKeyInactive, repo.updated.Status)
	require.Equal(t, []string{"sk-cc"}, cache.keys, "auth cache should be invalidated by key string")
}

func TestAdminService_AdminSetAPIKeyStatus_Enable(t *testing.T) {
	existing := &APIKey{ID: 7, Key: "sk-cc", Status: StatusAPIKeyInactive}
	repo := &apiKeyRepoStubForGroupUpdate{key: existing}
	cache := &authCacheInvalidatorStub{}
	svc := &adminServiceImpl{apiKeyRepo: repo, authCacheInvalidator: cache}

	got, err := svc.AdminSetAPIKeyStatus(context.Background(), 7, StatusAPIKeyActive)
	require.NoError(t, err)
	require.Equal(t, StatusAPIKeyActive, got.Status)
	require.Equal(t, []string{"sk-cc"}, cache.keys)
}

func TestAdminService_AdminSetAPIKeyStatus_RejectsSystemManagedStatus(t *testing.T) {
	repo := &apiKeyRepoStubForGroupUpdate{key: &APIKey{ID: 7, Key: "sk-cc", Status: StatusAPIKeyActive}}
	svc := &adminServiceImpl{apiKeyRepo: repo}

	for _, status := range []string{StatusAPIKeyExpired, StatusAPIKeyQuotaExhausted, StatusAPIKeyDisabled, "bogus", ""} {
		_, err := svc.AdminSetAPIKeyStatus(context.Background(), 7, status)
		require.Error(t, err, "status %q should be rejected", status)
	}
	require.Nil(t, repo.updated, "Update should not be called for rejected statuses")
}

func TestAdminService_AdminSetAPIKeyStatus_NotFound(t *testing.T) {
	repo := &apiKeyRepoStubForGroupUpdate{key: &APIKey{ID: 7}, getErr: ErrAPIKeyNotFound}
	svc := &adminServiceImpl{apiKeyRepo: repo}

	_, err := svc.AdminSetAPIKeyStatus(context.Background(), 7, StatusAPIKeyInactive)
	require.ErrorIs(t, err, ErrAPIKeyNotFound)
}

func TestAdminService_AdminSetAPIKeyStatus_UpdateError_SkipsCacheInvalidation(t *testing.T) {
	repo := &apiKeyRepoStubForGroupUpdate{key: &APIKey{ID: 7, Key: "sk-cc"}, updateErr: errors.New("db down")}
	cache := &authCacheInvalidatorStub{}
	svc := &adminServiceImpl{apiKeyRepo: repo, authCacheInvalidator: cache}

	_, err := svc.AdminSetAPIKeyStatus(context.Background(), 7, StatusAPIKeyInactive)
	require.Error(t, err)
	require.Empty(t, cache.keys, "cache must not be invalidated when the status write fails")
}

func TestAdminService_AdminSetAPIKeyStatus_NilInvalidator_NoPanic(t *testing.T) {
	repo := &apiKeyRepoStubForGroupUpdate{key: &APIKey{ID: 7, Key: "sk-cc", Status: StatusAPIKeyActive}}
	svc := &adminServiceImpl{apiKeyRepo: repo}

	got, err := svc.AdminSetAPIKeyStatus(context.Background(), 7, StatusAPIKeyInactive)
	require.NoError(t, err)
	require.Equal(t, StatusAPIKeyInactive, got.Status)
}
