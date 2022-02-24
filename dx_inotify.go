package tail

import (
	"github.com/fsnotify/fsnotify"
)

func (dx *Dx) inotify() {
	wa, err := fsnotify.NewWatcher()
	if err != nil {
		xEnv.Errorf("%s dx new inotify error", dx.dir, err)
		return
	}

	if er := wa.Add(dx.dir); er != nil {
		xEnv.Errorf("%s tx add inotify error %v", dx.dir, er)
		return
	}

	defer func() {
		wa.Remove(dx.dir)
		wa.Close()
	}()

	for {
		select {

		case <-dx.tom.Dying():
			xEnv.Errorf("%s dx inotify %v", dx.dir, dx.tom.Err())
			return

		case ev, ok := <-wa.Events:
			if !ok {
				return
			}

			switch {

			case ev.Op&fsnotify.Create == fsnotify.Create:
				dx.readDir()

			case ev.Op&fsnotify.Remove == fsnotify.Remove:
				dx.readDir()

			case ev.Op&fsnotify.Rename == fsnotify.Rename:
				return

			case ev.Op&fsnotify.Chmod == fsnotify.Chmod:
				dx.readDir()

				//todo
			}
		}
	}

}
