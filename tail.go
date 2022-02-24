package tail

import (
	"fmt"
	"github.com/rock-go/rock/lua"
	"gopkg.in/tomb.v2"
)

type tail struct {
	lua.Super
	cfg   *config
	queue chan line
	limit *limit
	tom   *tomb.Tomb
	rn    uint64
	wn    uint64
}

func newTail(cfg *config) *tail {
	return &tail{cfg: cfg}
}

func (t *tail) Name() string {
	return t.cfg.name
}

func (t *tail) Type() string {
	return typeof
}

func (t *tail) constructor() {

	//初始化tom
	t.tom = new(tomb.Tomb)

	//queue
	t.queue = make(chan line)

	//初始化限速
	t.limit = newLimit(t.cfg.limit)
}

func (t *tail) Start() error {

	if err := t.cfg.valid(); err != nil {
		return err
	}

	t.constructor()

	for i := 0; i < t.cfg.thread; i++ {
		go func(k int) { t.output(k) }(i)
	}

	return nil
}

func (t *tail) Close() error {
	//关闭队列
	close(t.queue)
	t.tom.Kill(fmt.Errorf("%s exit", t.Name()))
	return nil
}
