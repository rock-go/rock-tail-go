package tail

import (
	"context"
	"golang.org/x/time/rate"
)

type limit struct {
	ctx  context.Context
	stop context.CancelFunc
	rate *rate.Limiter
}

func newLimit(n int) *limit {
	if n <= 0 {
		return &limit{}
	}

	ctx, cancel := context.WithCancel(context.TODO())
	if n <= 0 {
		return &limit{rate: nil, ctx: ctx, stop: cancel}
	}
	return &limit{ctx, cancel, rate.NewLimiter(rate.Limit(n), n*2)}
}

func (l *limit) wait() {
	if l.rate == nil {
		return
	}
	l.rate.Wait(l.ctx)
}
