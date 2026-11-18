package ratelimiter

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// RedisEval executes a Lua script and returns its decoded Redis reply.
// For example, with go-redis, return client.Eval(ctx, script, keys, args...).Result().
type RedisEval func(ctx context.Context, script string, keys []string, args ...any) (any, error)

// RedisFixedWindow applies a per-key limit in fixed windows using Redis server time.
type RedisFixedWindow struct {
	eval     RedisEval
	prefix   string
	limit    int
	windowMS int64
}

// NewRedisFixedWindow requires a namespace dedicated to this algorithm and configuration.
// Instances sharing a quota must use the same namespace and configuration.
func NewRedisFixedWindow(eval RedisEval, namespace string, limit int, window time.Duration) (*RedisFixedWindow, error) {
	windowMS, err := fixedWindowConfig(limit, window)
	if err != nil {
		return nil, err
	}
	prefix, err := redisPrefix(eval, namespace, "fixed-window")
	if err != nil {
		return nil, err
	}
	return &RedisFixedWindow{eval: eval, prefix: prefix, limit: limit, windowMS: windowMS}, nil
}

func (p *RedisFixedWindow) Take(ctx context.Context, key string) (Decision, error) {
	return takeRedis(ctx, p.eval, redisFixedWindowScript, p.prefix+key, p.limit, p.windowMS)
}

// RedisTokenBucket starts each key with a full bucket and refills one token per interval.
type RedisTokenBucket struct {
	eval         RedisEval
	prefix       string
	refillMS     int64
	fullCreditMS int64
}

// NewRedisTokenBucket requires a namespace dedicated to this algorithm and configuration.
// Instances sharing a quota must use the same namespace and configuration.
func NewRedisTokenBucket(eval RedisEval, namespace string, capacity int, refill time.Duration) (*RedisTokenBucket, error) {
	if refill%time.Millisecond != 0 {
		return nil, errors.New("ratelimiter: Redis refill must be a whole number of milliseconds")
	}
	fullCredit, err := tokenBucketConfig(capacity, refill)
	if err != nil {
		return nil, err
	}
	prefix, err := redisPrefix(eval, namespace, "token-bucket")
	if err != nil {
		return nil, err
	}
	return &RedisTokenBucket{
		eval: eval, prefix: prefix, refillMS: refill.Milliseconds(), fullCreditMS: fullCredit.Milliseconds(),
	}, nil
}

func (p *RedisTokenBucket) Take(ctx context.Context, key string) (Decision, error) {
	return takeRedis(ctx, p.eval, redisTokenBucketScript, p.prefix+key, p.refillMS, p.fullCreditMS)
}

func redisPrefix(eval RedisEval, namespace, algorithm string) (string, error) {
	if eval == nil || namespace == "" {
		return "", errors.New("ratelimiter: Redis evaluator and namespace are required")
	}
	return fmt.Sprintf("ratelimiter:%s:%d:%s:", algorithm, len(namespace), namespace), nil
}

func takeRedis(ctx context.Context, eval RedisEval, script, key string, args ...any) (Decision, error) {
	if err := ctx.Err(); err != nil {
		return Decision{}, err
	}
	result, err := eval(ctx, script, []string{key}, args...)
	if err != nil {
		return Decision{}, fmt.Errorf("ratelimiter: Redis script: %w", err)
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return Decision{}, errors.New("ratelimiter: invalid Redis decision")
	}
	allowed, allowedOK := values[0].(int64)
	delayMS, delayOK := values[1].(int64)
	const maxDelayMS = int64((1<<63 - 1) / time.Millisecond)
	if !allowedOK || !delayOK || (allowed != 0 && allowed != 1) ||
		delayMS < 0 || delayMS > maxDelayMS || (allowed == 1 && delayMS != 0) {
		return Decision{}, errors.New("ratelimiter: invalid Redis decision")
	}
	return Decision{Allowed: allowed == 1, RetryAfter: time.Duration(delayMS) * time.Millisecond}, nil
}

const redisClockScript = `
local time = redis.call('TIME')
local now = time[1] * 1000 + math.floor(time[2] / 1000)
`

const redisFixedWindowScript = redisClockScript + `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local windowID = math.floor(now / window)
local remaining = window - now % window
local state = redis.call('HMGET', key, 'window', 'count')
local count = 0
if tonumber(state[1]) == windowID then
    count = tonumber(state[2])
end
if count >= limit then
    return {0, remaining}
end
redis.call('HSET', key, 'window', windowID, 'count', string.format('%.0f', count + 1))
redis.call('PEXPIREAT', key, now + remaining)
return {1, 0}
`

const redisTokenBucketScript = redisClockScript + `
local key = KEYS[1]
local refill = tonumber(ARGV[1])
local fullCredit = tonumber(ARGV[2])
local state = redis.call('HMGET', key, 'at', 'credit')
local credit = fullCredit
if state[1] then
    local at = tonumber(state[1])
    credit = math.min(fullCredit, tonumber(state[2]) + math.max(0, now - at))
    if credit < refill then
        return {0, refill - credit + math.max(0, at - now)}
    end
    now = math.max(now, at)
end
credit = credit - refill
redis.call('HSET', key, 'at', now, 'credit', credit)
redis.call('PEXPIREAT', key, now + fullCredit - credit)
return {1, 0}
`
