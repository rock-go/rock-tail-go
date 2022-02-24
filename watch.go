package tail

import (
	"context"
	"github.com/fsnotify/fsnotify"
	"path/filepath"
	"sync"
)

type OP = fsnotify.Op
type Event fsnotify.Event

type handle func(Event)

type watch struct {
	mu   sync.RWMutex
	file map[string]handle
	ctx  context.Context
	wer  *fsnotify.Watcher
}

func newW(ctx context.Context) (watch, error) {
	var wx watch

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return wx, err
	}

	wx = watch{
		ctx:  ctx,
		file: make(map[string]handle),
		wer:  w,
	}
	xEnv.Spawn(0, wx.loop)

	return wx, nil
}

func (w *watch) call(ev Event) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if hde, ok := w.file[ev.Name]; ok {
		hde(ev)
		return
	}

	if hde, ok := w.file[filepath.Dir(ev.Name)]; ok {
		hde(ev)
		return
	}
}

func (w *watch) insert(path string, hde handle) {
	if hde == nil {
		return
	}

	if w.wer == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.file[path]; ok {
		xEnv.Errorf("%s file watcher already ok", path)
		return
	}

	if e := w.wer.Add(path); e != nil {
		xEnv.Errorf("%s file watcher add error %v", path, e)
		return
	}
	w.file[path] = hde
}

func (w *watch) remove(path string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.file, path)
	w.wer.Remove(path)
}

func (w *watch) free() {
	w.wer.Close()
	w.file = nil
	w.wer = nil
}

func (w *watch) loop() {
	defer w.free()

	for {
		select {
		case <-w.ctx.Done():
			return
		case ev, ok := <-w.wer.Events:
			if !ok {
				continue
			}
			w.call(Event(ev))

		case err, ok := <-w.wer.Errors:
			if !ok {
				continue
			}
			xEnv.Errorf("watch %v", err)

		}
	}
}
