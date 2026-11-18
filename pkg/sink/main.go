package sink

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/replicase/pgcapture/pkg/cursor"
	"github.com/replicase/pgcapture/pkg/source"
)

type CleanFn func()
type ApplyFn func(sourceRemaining int, message source.Change, committed chan cursor.Checkpoint) error

type Sink interface {
	Setup() (cp cursor.Checkpoint, err error)
	Apply(changes chan source.Change) (committed chan cursor.Checkpoint)
	Error() error
	Stop() error
}

type BaseSink struct {
	CleanFn   CleanFn
	cleanOnce sync.Once

	committed chan cursor.Checkpoint
	state     int64
	err       atomic.Value
	// cleaned is closed once CleanFn has fully returned, so callers (and tests)
	// can wait for cleanup to finish before reusing or replacing the struct.
	cleaned chan struct{}
	// done is closed when the background apply goroutine has fully exited,
	// including draining its input channel. Reusing the sink is only safe once
	// this has fired.
	done chan struct{}
}

func (b *BaseSink) Setup() (cp cursor.Checkpoint, err error) {
	panic("implement me")
}

func (b *BaseSink) Apply(changes chan source.Change) (committed chan cursor.Checkpoint) {
	panic("implement me")
}

func (b *BaseSink) apply(changes chan source.Change, applyFn ApplyFn) (committed chan cursor.Checkpoint) {
	if !atomic.CompareAndSwapInt64(&b.state, 0, 1) {
		return nil
	}
	b.committed = make(chan cursor.Checkpoint, 1000)
	b.cleaned = make(chan struct{})
	b.done = make(chan struct{})
	atomic.StoreInt64(&b.state, 2)

	go func() {
		defer close(b.done)
		ticker := time.NewTicker(time.Second)
		for atomic.LoadInt64(&b.state) == 2 {
			select {
			case change, more := <-changes:
				if !more {
					goto cleanup
				}
				if err := applyFn(len(changes), change, b.committed); err != nil {
					b.err.Store(fmt.Errorf("%w", err))
					goto cleanup
				}
			case <-ticker.C:
			}
		}
	cleanup:
		ticker.Stop()
		atomic.StoreInt64(&b.state, 4)
		// run cleanup inline rather than in a detached goroutine so that, by the
		// time this goroutine exits (and done closes), CleanFn and its internal
		// synchronization have fully completed. A later explicit Stop() reuses
		// the same cleanOnce and becomes a no-op.
		b.cleanOnce.Do(func() {
			b.CleanFn()
			close(b.committed)
			if b.cleaned != nil {
				close(b.cleaned)
			}
		})
		for range changes {
			// this loop should do nothing and only exit when the input channel is closed
		}
	}()
	return b.committed
}

func (b *BaseSink) Error() error {
	if err, ok := b.err.Load().(error); ok {
		return err
	}
	return nil
}

func (b *BaseSink) Stop() error {
	switch atomic.LoadInt64(&b.state) {
	case 0:
		b.cleanOnce.Do(b.CleanFn)
	case 1, 2:
		for !atomic.CompareAndSwapInt64(&b.state, 2, 3) {
			runtime.Gosched()
		}
		fallthrough
	case 3:
		for atomic.LoadInt64(&b.state) != 4 {
			time.Sleep(time.Millisecond * 100)
		}
		fallthrough
	case 4:
		b.cleanOnce.Do(func() {
			b.CleanFn()
			close(b.committed)
			// signal that CleanFn (and its internal synchronization) has fully
			// completed. Stop may be invoked more than once (the apply loop also
			// calls it internally), so close inside the same once-guard.
			if b.cleaned != nil {
				close(b.cleaned)
			}
		})
	}
	return b.Error()
}

// Cleaned returns a channel that is closed once the sink's CleanFn has fully
// returned during Stop. It returns nil if Apply was never called.
func (b *BaseSink) Cleaned() <-chan struct{} {
	return b.cleaned
}

// Done returns a channel closed when the background apply goroutine has fully
// exited (including draining its input). It returns nil if Apply was never
// called.
func (b *BaseSink) Done() <-chan struct{} {
	return b.done
}

// Reset re-arms a stopped sink so the same struct can be Setup and Applied
// again without replacing its value (which would race a still-finishing
// background goroutine). It blocks until the previous apply goroutine has fully
// exited and must only be called after Stop.
func (b *BaseSink) Reset() {
	if b.done != nil {
		<-b.done
	}
	b.cleanOnce = sync.Once{}
	b.committed = nil
	atomic.StoreInt64(&b.state, 0)
	b.err = atomic.Value{}
	b.cleaned = nil
	b.done = nil
}
