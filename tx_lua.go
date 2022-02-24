package tail

import (
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/xlib"
	"time"
)

func (tx *Tx) ToLValue() lua.LValue {
	return lua.NewAnyData(tx)
}

func (tx *Tx) renameL(L *lua.LState) int {
	tx.fx.path = auxlib.LSprintf(L, 0)
	return 0
}

func (tx *Tx) afterL(L *lua.LState) int {
	n := L.IsInt(1)
	if n > 0 {
		tx.after = time.Duration(n) * time.Second
	}
	return 0
}

func (tx *Tx) fileL(L *lua.LState) int {
	return 0
}

func (tx *Tx) exitL(L *lua.LState) int {
	tx.fx.exit()
	return 0
}

func (tx *Tx) pollL(L *lua.LState) int {
	td := 5 * time.Second
	n := L.IsInt(1)
	if n > 0 {
		td = time.Duration(n) * time.Millisecond
	}

	c := L.IsInt(2)
	tm := tx.timeout
	if c > 0 {
		tm = time.Second * time.Duration(c)
	}

	xEnv.Spawn(0, func() { tx.poll(td, tm) })
	return 0
}

func (tx *Tx) inotifyL(L *lua.LState) int {
	n := L.IsInt(1)
	td := tx.timeout
	if n > 0 {
		td = time.Duration(n) * time.Second
	}

	xEnv.Spawn(0, func() { tx.inotify(td) })
	return 0
}

func (tx *Tx) Index(L *lua.LState, key string) lua.LValue {
	switch key {

	case "rename":
		return L.NewFunction(tx.renameL)

	case "time":
		return xlib.NewTime(tx.fx.stime)

	case "exit":
		return L.NewFunction(tx.exitL)

	case "poll":
		return L.NewFunction(tx.pollL)

	case "after":
		return L.NewFunction(tx.afterL)

	case "inotify":
		return L.NewFunction(tx.inotifyL)

	}
	return nil
}
