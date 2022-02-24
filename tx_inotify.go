package tail

import (
	"github.com/fsnotify/fsnotify"
	"time"
)

func (tx *Tx) inotify(td time.Duration) {
	if tx.after > 0 {
		time.Sleep(tx.after)
	}

	wa, err := fsnotify.NewWatcher()
	if err != nil {
		xEnv.Errorf("%s tx new inotify error", tx.fx.path, err)
		return
	}

	if er := wa.Add(tx.fx.path); er != nil {
		xEnv.Errorf("%s tx add inotify error %v", tx.fx.path, er)
		return
	}

	tk := time.NewTicker(td)
	defer func() {
		wa.Remove(tx.fx.path)
		wa.Close()
		tk.Stop()
	}()

	for {
		select {
		case <-tk.C:
			xEnv.Errorf("%s inotify timeout", tx.fx.path)
			return

		case <-tx.fx.tom.Dying():
			xEnv.Errorf("%s inotify %v", tx.fx.path, tx.fx.tom.Err())
			return

		case ev, ok := <-wa.Events:
			if !ok {
				return
			}

			switch {
			case ev.Op&fsnotify.Remove == fsnotify.Remove:
				fallthrough

			case ev.Op&fsnotify.Rename == fsnotify.Rename:
				return

			case ev.Op&fsnotify.Chmod == fsnotify.Chmod:
				fallthrough

			case ev.Op&fsnotify.Write == fsnotify.Write:
				tx.reopen()
				return
			}
		}
	}
}
