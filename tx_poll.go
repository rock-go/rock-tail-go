package tail

import (
	"os"
	"time"
)

func (tx *Tx) poll(td time.Duration, timeout time.Duration) {
	if tx.stat == nil {
		return
	}

	if tx.after > 0 {
		time.Sleep(tx.after)
	}

	tk := time.NewTicker(td)
	defer tk.Stop()

	tm := time.NewTicker(timeout)
	defer tm.Stop()

	for {
		select {
		case <-tm.C:
			xEnv.Errorf("%s tx poll timeout", tx.fx.path)
			return

		case <-tk.C:
			st, er := os.Stat(tx.fx.path)

			if er != nil {
				if os.IsNotExist(er) {
					xEnv.Errorf("%s file not found", tx.fx.path)
					return
				}
				xEnv.Errorf("%s file open stat error %v", tx.fx.path, er)
				continue
			}

			//是否修改
			if tx.stat.ModTime().Unix() == st.ModTime().Unix() {
				continue
			}

			if st.Size() != tx.stat.Size() {
				tx.reopen()
				continue
			}

		case <-tx.fx.tom.Dying():
			xEnv.Errorf("%s poll exit succeed", tx.fx.path)
			return
		}
	}
}
