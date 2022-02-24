package tail

import (
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/pipe"
)

type config struct {
	name   string
	limit  int
	thread int

	add func([]byte) []byte
	enc func([]byte) []byte

	co   *lua.LState
	sdk  lua.Writer
	pipe []pipe.Pipe
}

func newConfig(L *lua.LState) *config {
	val := L.Get(1)
	cfg := &config{
		co:     xEnv.Clone(L),
		thread: 2,
		limit:  0,
	}

	switch val.Type() {
	case lua.LTString:
		cfg.name = val.String()

	case lua.LTTable:
		val.(*lua.LTable).Range(func(key string, val lua.LValue) {
			switch key {
			case "name":
				cfg.name = auxlib.CheckProcName(val, L)
			case "limit":
				cfg.limit = lua.IsInt(val)
			case "thread":
				cfg.thread = lua.CheckInt(L, val)
			case "to":
				cfg.sdk = auxlib.CheckWriter(val, L)

			default:
				//todo
			}
		})

	}

	return cfg
}

func (cfg *config) valid() error {
	if e := auxlib.Name(cfg.name); e != nil {
		return e
	}
	return nil
}
