package ratelimiter

import (
	"context"
	"time"
)

type Decision struct {
	Allowed    bool
	RetryAfter time.Duration // 0, если время следующей попытки неизвестно.
}

// Policy за одну атомарную операцию решает, можно ли пропустить
// запрос, и учитывает его, если он разрешён.
type Policy interface {
	Take(ctx context.Context, key string) (Decision, error)
}

type Limiter struct {
	policy Policy
}

func New(policy Policy) *Limiter {
	return &Limiter{policy: policy}
}

// Allow запрашивает одну единицу лимита для указанного ключа.
func (l *Limiter) Allow(ctx context.Context, key string) (Decision, error) {
	return l.policy.Take(ctx, key)
}
