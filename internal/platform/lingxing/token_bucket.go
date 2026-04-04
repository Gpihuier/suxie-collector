package lingxing

import (
	"sync"
	"time"
)

type tokenBucket struct {
	sem     chan struct{}
	timeout time.Duration
}

// newTokenBucket 创建固定容量信号量桶。
func newTokenBucket(capacity int, timeout time.Duration) *tokenBucket {
	if capacity <= 0 {
		capacity = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	return &tokenBucket{sem: make(chan struct{}, capacity), timeout: timeout}
}

// acquire 尝试获取令牌。
// 返回 release 回调，调用方必须在请求结束后释放令牌。
func (b *tokenBucket) acquire() (func(), bool) {
	select {
	case b.sem <- struct{}{}:
		releaseOnce := &sync.Once{}
		release := func() {
			releaseOnce.Do(func() {
				select {
				case <-b.sem:
				default:
				}
			})
		}
		// 异常兜底：2 分钟后自动回收，避免令牌泄漏导致“假死限流”。
		time.AfterFunc(b.timeout, release)
		return release, true
	default:
		return nil, false
	}
}
