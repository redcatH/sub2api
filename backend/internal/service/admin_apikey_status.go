package service

import (
	"context"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

// AdminSetAPIKeyStatus 管理员启用/停用 API Key。
//
// status 仅接受 active / inactive，与用户侧 PUT /api/keys/:id 的状态契约保持一致；
// expired / quota_exhausted 由系统按到期时间与用量自动管理，不允许手动设置。
// 状态切换后清除该 key 的认证缓存，保证下一次请求立即按新状态鉴权。
func (s *adminServiceImpl) AdminSetAPIKeyStatus(ctx context.Context, keyID int64, status string) (*APIKey, error) {
	if status != StatusAPIKeyActive && status != StatusAPIKeyInactive {
		return nil, infraerrors.BadRequest("INVALID_STATUS", "status must be active or inactive")
	}

	apiKey, err := s.apiKeyRepo.GetByID(ctx, keyID)
	if err != nil {
		return nil, err
	}

	apiKey.Status = status
	if err := s.apiKeyRepo.Update(ctx, apiKey, APIKeyUpdateFields{Status: true}); err != nil {
		return nil, err
	}

	if s.authCacheInvalidator != nil {
		s.authCacheInvalidator.InvalidateAuthCacheByKey(ctx, apiKey.Key)
	}

	return apiKey, nil
}
