package tail

import (
	"os"
	"time"
)

type Tx struct {
	fx      *Fx
	stat    os.FileInfo
	raw     []byte
	after   time.Duration
	timeout time.Duration
}

func newTx(fx *Fx, raw []byte) *Tx {
	tx := &Tx{
		fx:      fx,
		raw:     raw,
		timeout: 20 * 365 * 24 * 3600 * time.Second,
	}

	stat, e := fx.fd.Stat()
	if e == nil {
		tx.stat = stat
		return tx
	}

	xEnv.Errorf("%s new tx stat got fail %v", fx.path, e)
	return tx
}

func (tx *Tx) reopen() {

	if err := tx.fx.openFile(); err != nil {
		xEnv.Errorf("%s file path reopen fail error %v", tx.fx.path, err)
		return
	}
	xEnv.Errorf("%s file path reopen succeed", tx.fx.path)

}
