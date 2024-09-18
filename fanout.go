package fanout

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"

	"github.com/go-kratos/kratos/v2/log"
)

var (
	// ErrFull chan full.
	ErrFull = errors.New("fanout: chan full")
)

type Options struct {
	worker int
	buffer int
}

// Option fanout option
type Option func(*Options)

// Worker specifies the worker of fanout
func Worker(n int) Option {
	if n <= 0 {
		panic("fanout: worker should > 0")
	}
	return func(o *Options) {
		o.worker = n
	}
}

// Buffer specifies the buffer of fanout
func Buffer(n int) Option {
	if n <= 0 {
		panic("fanout: buffer should > 0")
	}
	return func(o *Options) {
		o.buffer = n
	}
}

type Fanout struct {
	name    string
	ch      chan *item
	options *Options
	waiter  sync.WaitGroup

	ctx    context.Context
	cancel func()
}

// New new a fanout struct.
func New(name string, opts ...Option) *Fanout {
	if name == "" {
		name = "anonymous"
	}
	o := &Options{
		worker: 1,
		buffer: 1024,
	}
	for _, op := range opts {
		op(o)
	}

	c := &Fanout{
		ch:      make(chan *item, o.buffer),
		name:    name,
		options: o,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	c.waiter.Add(o.worker)
	for i := 0; i < o.worker; i++ {
		go c.proc()
	}
	return c
}

type item struct {
	f   func(c context.Context)
	ctx context.Context
}

func (c *Fanout) proc() {
	defer c.waiter.Done()
	for {
		t := <-c.ch
		if t == nil {
			return
		}
		wrapFunc(t.f)(t.ctx)
	}
}

func wrapFunc(f func(c context.Context)) (res func(context.Context)) {
	res = func(ctx context.Context) {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 64*1024)
				buf = buf[:runtime.Stack(buf, false)]
				_, _ = fmt.Fprintf(os.Stderr, "fanout: panic recovered: %s\n%s\n", r, buf)
				log.Context(ctx).Errorf("panic in fanout proc, err: %s, stack: %s", r, buf)
			}
		}()
		f(ctx)
	}
	return
}

// Do save a callback func with channel full err
func (c *Fanout) Do(ctx context.Context, f func(ctx context.Context)) (err error) {
	if c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	select {
	case c.ch <- &item{f: f, ctx: Detach(ctx)}:
	default:
		err = ErrFull
	}
	return
}

// SyncDo save a callback func no channel full err
func (c *Fanout) SyncDo(ctx context.Context, f func(ctx context.Context)) (err error) {
	if c.ctx.Err() != nil {
		return c.ctx.Err()
	}
	select {
	case c.ch <- &item{f: f, ctx: Detach(ctx)}:
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

// Close close fanout
func (c *Fanout) Close() error {
	if err := c.ctx.Err(); err != nil {
		return err
	}
	c.cancel()
	for i := 0; i < c.options.worker; i++ {
		c.ch <- nil
	}
	c.waiter.Wait()
	return nil
}
