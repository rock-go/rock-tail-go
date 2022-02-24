package tail

import "time"

func (dx *Dx) poll(td time.Duration) {
	tk := time.NewTicker(td)
	defer tk.Stop()

	for {
		select {
		case <-tk.C:
			dx.readDir()

		case <-dx.tom.Dying():
			xEnv.Errorf("%s/%s dx %v", dx.dir, dx.base, dx.tom.Err())
			return
		}
	}

}
