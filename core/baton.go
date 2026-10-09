package core

import "sync"

// baton is a channel-based mutex. This allows for using it as part of a select
// statement. It is used by the scheduler to hand control of dispatching back
// and forth between the (polling) ping loop and the streaming ping loop.
type baton struct {
	mu     sync.Mutex
	holder string
	ch     chan struct{}
}

// newBaton creates a new baton for sharing among actors, each identified
// by a non-empty string. The baton is initially not held by anything.
func newBaton() *baton {
	b := &baton{
		ch: make(chan struct{}, 1),
	}
	b.ch <- struct{}{}
	return b
}

// HeldBy reports if the actor specified by the argument holds the baton.
func (b *baton) HeldBy(by string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.holder == by
}

// Acquire returns a channel that receives when the baton is acquired by the
// caller. Be sure to call [baton.Acquired] after receiving from the channel.
func (b *baton) Acquire() <-chan struct{} {
	// b.ch should never change, so no need to lock around it
	return b.ch
}

// Acquired must be called by the actor that successfully acquired the baton
// immediately after acquiring it. It panics if the baton is already marked as
// held.
func (b *baton) Acquired(by string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.holder != "" {
		panic("baton already held by " + b.holder)
	}
	b.holder = by
}

// Release releases the baton, if it is held by the argument.
func (b *baton) Release(by string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.holder != by {
		return
	}
	b.holder = ""
	b.ch <- struct{}{}
}
