package fanout

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestFanout_Do(t *testing.T) {
	ca := New("cache", Worker(1), Buffer(1024))
	var run bool
	ca.Do(context.Background(), func(c context.Context) {
		run = true
		panic("error")
	})
	time.Sleep(time.Millisecond * 50)
	t.Log("not panic")
	if !run {
		t.Fatal("expect run be true")
	}
}

func TestFanout_Close(t *testing.T) {
	ctx := context.Background()
	ca := New("cache", Worker(1), Buffer(1024))
	var count int64
	for i := 0; i < 5; i++ {
		ca.Do(ctx, func(ctx context.Context) {
			atomic.AddInt64(&count, 1)
		})
	}
	ca.Close()
	if err := ca.Do(ctx, func(c context.Context) {}); err == nil {
		t.Fatal("expect get err")
	}
	if count != 5 {
		t.Fatal("expect count equal 5")
	}
}

func TestFanout_SyncDo(t *testing.T) {
	ca := New("cache", Worker(1), Buffer(1))
	var run bool
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
	defer cancel()
	err := ca.SyncDo(ctx, func(c context.Context) {
		time.Sleep(time.Millisecond * 200)
	})
	if err != nil {
		t.Fatal("expect err be nil")
	}
	err = ca.SyncDo(ctx, func(c context.Context) {
		run = true
	})
	if err != ctx.Err() {
		t.Fatal("expect err be context deadline")
	}
	time.Sleep(time.Millisecond * 60)
	if run == true {
		t.Fatal("expect run is false")
	}
}
