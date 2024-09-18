package fanout

import (
	"context"
	"time"
)

func Detach(ctx context.Context) context.Context {
	return detached{ctx: ctx}
}

type detached struct {
	ctx context.Context
}

func (detached) Deadline() (time.Time, bool) {
	return time.Time{}, false
}

func (d detached) Done() <-chan struct{} {
	return nil
}

func (d detached) Err() error {
	return nil
}

func (d detached) Value(key interface{}) interface{} {
	return d.ctx.Value(key)
}
