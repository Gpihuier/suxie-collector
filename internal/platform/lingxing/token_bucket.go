package lingxing

import (
	"sync"
	"time"
)

type tokenBucket struct {
	sem     chan struct{}
	timeout time.Duration
}

func newTokenBucket(capacity int, timeout time.Duration) *tokenBucket {
	if capacity <= 0 {
		capacity = 1
	}
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	return &tokenBucket{sem: make(chan struct{}, capacity), timeout: timeout}
}

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
		time.AfterFunc(b.timeout, release)
		return release, true
	default:
		return nil, false
	}
}
