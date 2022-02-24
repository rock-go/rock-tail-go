package tail

import (
	"bufio"
	"github.com/rock-go/rock/bucket"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/pipe"
	"gopkg.in/tomb.v2"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Fx struct {
	path  string
	bkt   []string
	delim byte
	err   error

	fd *os.File
	rd *bufio.Reader

	buffer int
	seek   int64
	stat   os.FileInfo
	stime  time.Time

	enc    func([]byte) []byte
	add    func([]byte) []byte
	handle func(line, error)

	watch bool
	poll  time.Duration
	wait  time.Duration

	on pipe.Pipe
	co *lua.LState

	pipe []pipe.Pipe

	tom *tomb.Tomb
	tag map[string]lua.LValue
}

func (fx *Fx) bucket() *bucket.Bucket {
	if len(fx.bkt) == 0 {
		return nil
	}
	return bucket.Pack(xEnv, fx.bkt...)
}

func (fx *Fx) openFile() error {
	fd, err := openFile(fx.path)
	if err != nil {
		fx.err = err
		return err
	}

	fx.fd = fd
	fx.rd = bufio.NewReaderSize(fd, fx.buffer)
	fx.offset()
	fx.err = nil
	fx.stime = time.Now()

	xEnv.Spawn(0, fx.readline)
	xEnv.Infof("%s fx open succeed record [%d]", fx.path, fx.seek)
	return nil
}

func (fx *Fx) open() error {

	//是否开启等待
	if fx.wait < 0 {
		return fx.openFile()
	}

	er := fx.openFile()
	if er == nil {
		return nil
	}

	tk := time.NewTicker(fx.wait)
	defer tk.Stop()

	for {

		select {

		case <-tk.C:
			er = fx.openFile()
			if er == nil {
				return nil
			}
			xEnv.Errorf("%s open file fail %v", fx.path, er)

		case <-fx.tom.Dying():
			xEnv.Errorf("%s wait exit", fx.path)
			return nil

		}
	}

	return nil
}

func (fx *Fx) save() {
	seek, e := fx.fd.Seek(0, io.SeekCurrent)
	if e != nil {
		xEnv.Infof("%s current seek error %v", fx.path, e)
		return
	}

	if fx.fd == nil {
		xEnv.Errorf("current %s file is nil", fx.path)
		return
	}

	bkt := fx.bucket()
	if bkt == nil {
		xEnv.Errorf("%s fx current bucket empty", fx.path)
		return
	}

	err := bkt.Store(fx.path, seek, 0)
	if err != nil {
		xEnv.Errorf("save %s seek record error %v", fx.path, err)
		return
	}

	xEnv.Infof("%s save seek record [%d]", fx.path, seek)
}

func (fx *Fx) offset() {

	if fx.fd == nil {
		xEnv.Infof("tail %s fd not found", fx.path)
		return
	}

	bkt := fx.bucket()
	if bkt == nil {
		xEnv.Errorf("%s fx current bucket empty", fx.path)
		return
	}

	seek := bkt.Int64(fx.path)
	stat, _ := fx.fd.Stat()
	size := stat.Size()
	if seek > size {
		xEnv.Infof("tail offset record [%d] > [%d]", seek, size)
		fx.seek = 0
	} else {
		fx.seek = seek
	}

	fx.fd.Seek(fx.seek, 0)
	xEnv.Infof("%s tail position of %d", fx.path, fx.seek)
	return

}

func (fx *Fx) onEOF(raw []byte) {
	if fx.on == nil {
		return
	}

	if e := fx.on(newTx(fx, raw), fx.co); e != nil {
		xEnv.Errorf("%s on eof pipe error %v", fx.path, e)
	}
}

func (fx *Fx) onRead(raw []byte) {
	if len(raw) == 0 {
		return
	}

	pipe.Do(fx.pipe, raw, fx.co, func(err error) {
		xEnv.Errorf("%s on read pipe call fail %v", fx.path, err)
	})
}

func (fx *Fx) Handle(raw []byte) {

	rn := len(raw)
	if rn <= 1 {
		return
	}

	if raw[rn-1] == fx.delim {
		raw = raw[:rn-1]
	}

	fx.onRead(raw)
	fx.handle(line{raw, fx.enc, fx.add}, nil)
}

func (fx *Fx) close() {
	_ = fx.fd.Close()
}

func (fx *Fx) exit() {
	fx.on = nil
	xEnv.Errorf("%s tx exit when eof", fx.path)
}

func (fx *Fx) readline() {
	defer fx.close()

	for {
		select {

		case <-fx.tom.Dying():
			fx.save()
			xEnv.Errorf("%s readline exit", fx.path)
			return

		default:
			raw, err := fx.rd.ReadBytes(fx.delim)
			switch err {

			case nil:
				fx.Handle(raw)

			case io.EOF:
				fx.Handle(raw)
				fx.save()
				fx.onEOF(raw)
				xEnv.Infof("%s file eof", fx.path)
				//轮询监控
				return

			default:
				xEnv.Errorf("%s read line raw error %v", fx.path, err)
				//todo
			}
		}
	}

}

func newFx(tom *tomb.Tomb, value string, handle func(line, error)) *Fx {
	path, e := filepath.Abs(filepath.Clean(value))
	if e != nil {
		path = filepath.Clean(value)
	}

	return &Fx{
		delim:  '\n',
		poll:   -1,
		wait:   -1,
		buffer: 4096,
		tom:    tom,
		path:   path,
		handle: handle,
	}
}
