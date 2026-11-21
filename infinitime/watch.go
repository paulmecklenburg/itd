package infinitime

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sync"

	"tinygo.org/x/bluetooth"
)

type notifier interface {
	notify([]byte)
	setChar(*bluetooth.DeviceCharacteristic)
	swapChar(*bluetooth.DeviceCharacteristic) *bluetooth.DeviceCharacteristic
}

type watcher[T any] struct {
	mu         sync.Mutex
	nextFuncID int
	callbacks  map[int]func(T, error)
	char       *bluetooth.DeviceCharacteristic
}

func (w *watcher[T]) swapChar(c *bluetooth.DeviceCharacteristic) *bluetooth.DeviceCharacteristic {
	w.mu.Lock()
	old := w.char
	w.char = c
	w.mu.Unlock()
	return old
}

func (w *watcher[T]) setChar(c *bluetooth.DeviceCharacteristic) {
	if old := w.swapChar(c); old != nil && old != c {
		_ = old.EnableNotifications(nil)
	}
}

func (w *watcher[T]) addCallback(fn func(T, error)) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	funcID := w.nextFuncID
	w.callbacks[funcID] = fn
	w.nextFuncID++
	return funcID
}

func (w *watcher[T]) notify(b []byte) {
	var val T
	err := binary.Read(bytes.NewReader(b), binary.LittleEndian, &val)
	w.mu.Lock()
	for _, fn := range w.callbacks {
		go fn(val, err)
	}
	w.mu.Unlock()
}

func (w *watcher[T]) cancelFn(d *Device, ch btChar, id int) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			var charToDisable *bluetooth.DeviceCharacteristic

			d.notifierMtx.Lock()
			w.mu.Lock()
			delete(w.callbacks, id)
			if len(w.callbacks) == 0 {
				if cur, ok := d.notifierMap[ch]; ok && cur == any(w) {
					delete(d.notifierMap, ch)
					charToDisable = w.char
					w.char = nil
				}
			}
			w.mu.Unlock()
			d.notifierMtx.Unlock()

			if charToDisable != nil {
				d.disableNotifications(ch, charToDisable)
			}
		})
	}
}

func watchChar[T any](ctx context.Context, d *Device, ch btChar, fn func(T, error)) error {
	if d.updating.Load() {
		return fmt.Errorf("device is currently updating")
	}

	d.notifierMtx.Lock()

	if n, ok := d.notifierMap[ch]; ok {
		w := n.(*watcher[T])
		funcID := w.addCallback(fn)
		d.notifierMtx.Unlock()
		context.AfterFunc(ctx, w.cancelFn(d, ch, funcID))
		return nil
	}

	w := &watcher[T]{callbacks: map[int]func(T, error){}}
	funcID := w.addCallback(fn)
	d.notifierMap[ch] = w
	d.notifierMtx.Unlock()

	context.AfterFunc(ctx, w.cancelFn(d, ch, funcID))

	// Register first so a transient disconnect during startup still lets reconnect restore it.
	if c, err := d.getChar(ch); err == nil {
		if err := d.enableNotifications(ch, c, w.notify); err == nil {
			var old *bluetooth.DeviceCharacteristic
			d.notifierMtx.Lock()
			stillRegistered := d.notifierMap[ch] == any(w)
			if stillRegistered {
				old = w.swapChar(c)
			}
			d.notifierMtx.Unlock()

			if stillRegistered {
				if old != nil && old != c {
					_ = old.EnableNotifications(nil)
				}
			} else {
				d.disableNotifications(ch, c)
			}
		}
	}

	return nil
}

func watchCharOnce[T any](d *Device, ch btChar, fn func(T)) error {
	if !d.IsConnected() {
		return fmt.Errorf("device is not connected")
	}
	disconnectCh := d.getDisconnectCh()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var watchErr error
	err := watchChar(ctx, d, ch, func(val T, err error) {
		defer cancel()
		if err != nil {
			watchErr = err
			return
		}
		fn(val)
	})
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return watchErr
	case <-disconnectCh:
		return fmt.Errorf("device disconnected")
	}
}
