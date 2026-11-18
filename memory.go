package ratelimiter

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryFixedWindow applies a per-key limit in fixed windows aligned to the Unix epoch.
type MemoryFixedWindow struct {
	mu       sync.Mutex
	windowMS int64
	limit    int
	started  bool
	windowID int64
	counts   map[string]int
}

func NewMemoryFixedWindow(limit int, window time.Duration) (*MemoryFixedWindow, error) {
	windowMS, err := fixedWindowConfig(limit, window)
	if err != nil {
		return nil, err
	}
	return &MemoryFixedWindow{windowMS: windowMS, limit: limit, counts: make(map[string]int)}, nil
}

func (p *MemoryFixedWindow) Take(ctx context.Context, key string) (Decision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}

	now := time.Now().UnixMilli()
	windowID := now / p.windowMS
	if !p.started || p.windowID != windowID {
		p.started = true
		p.windowID = windowID
		p.counts = make(map[string]int)
	}
	if p.counts[key] >= p.limit {
		return Decision{RetryAfter: time.Duration(p.windowMS-now%p.windowMS) * time.Millisecond}, nil
	}
	p.counts[key]++
	return Decision{Allowed: true}, nil
}

// MemoryTokenBucket starts each key with a full bucket and refills one token per interval.
type MemoryTokenBucket struct {
	mu          sync.Mutex
	refill      time.Duration
	fullCredit  time.Duration
	nextCleanup time.Time
	buckets     map[string]memoryBucket
}

type memoryBucket struct {
	credit time.Duration
	at     time.Time
}

func NewMemoryTokenBucket(capacity int, refill time.Duration) (*MemoryTokenBucket, error) {
	fullCredit, err := tokenBucketConfig(capacity, refill)
	if err != nil {
		return nil, err
	}
	return &MemoryTokenBucket{
		refill: refill, fullCredit: fullCredit, buckets: make(map[string]memoryBucket),
	}, nil
}

func (p *MemoryTokenBucket) Take(ctx context.Context, key string) (Decision, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}

	now := time.Now()
	if !now.Before(p.nextCleanup) {
		for k, bucket := range p.buckets {
			if now.Sub(bucket.at) >= p.fullCredit-bucket.credit {
				delete(p.buckets, k)
			}
		}
		p.nextCleanup = now.Add(time.Minute)
	}
	credit := p.fullCredit
	if bucket, ok := p.buckets[key]; ok {
		elapsed := now.Sub(bucket.at)
		if elapsed < p.fullCredit-bucket.credit {
			credit = bucket.credit + elapsed
		}
	}
	if credit < p.refill {
		return Decision{RetryAfter: p.refill - credit}, nil
	}
	p.buckets[key] = memoryBucket{credit: credit - p.refill, at: now}
	return Decision{Allowed: true}, nil
}

func fixedWindowConfig(limit int, window time.Duration) (int64, error) {
	if limit <= 0 || int64(limit) > 1<<53-1 {
		return 0, errors.New("ratelimiter: limit must be between 1 and 2^53-1")
	}
	if window <= 0 || window%time.Millisecond != 0 {
		return 0, errors.New("ratelimiter: window must be a positive whole number of milliseconds")
	}
	return window.Milliseconds(), nil
}

func tokenBucketConfig(capacity int, refill time.Duration) (time.Duration, error) {
	const maxDuration = time.Duration(1<<63 - 1)
	if capacity <= 0 || refill <= 0 || int64(capacity) > int64(maxDuration/refill) {
		return 0, errors.New("ratelimiter: capacity and refill must be positive and their product must fit in time.Duration")
	}
	return time.Duration(capacity) * refill, nil
}
