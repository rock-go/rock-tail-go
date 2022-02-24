package tail

import (
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/pipe"
)

func (t *tail) ret(L *lua.LState) int {
	L.Push(L.NewLightUserData(t))
	return 1
}

func (t *tail) checkVM(L *lua.LState) bool {
	cu, nu := t.cfg.co.CodeVM(), L.CodeVM()
	if cu != nu {
		L.RaiseError("%s proc start must be %s , but %s", t.Name(), cu, nu)
		return false
	}
	return true
}

func (t *tail) pipeL(L *lua.LState) int {
	t.cfg.pipe = pipe.Check(L)
	return t.ret(L)
}

func (t *tail) fileL(L *lua.LState) int {
	path := auxlib.LSprintf(L, 0)
	if path == "." || path == "" || path == ".." {
		return 0
	}
	L.Push(t.newFx(path))
	return 1
}

func (t *tail) dirL(L *lua.LState) int {
	dir := L.CheckString(1)
	base := L.CheckString(2)
	L.Push(newDx(dir, base, t.tom, t.newFx))
	return 1
}

func (t *tail) startL(L *lua.LState) int {
	if t.checkVM(L) {
		xEnv.Start(t, func(err error) {
			L.RaiseError("%v", err)
		})
	}
	return t.ret(L)
}

func (t *tail) limitL(L *lua.LState) int {
	t.cfg.limit = L.IsInt(1)
	return t.ret(L)
}

func (t *tail) toL(L *lua.LState) int {
	t.cfg.sdk = auxlib.CheckWriter(L.Get(1), L)
	return t.ret(L)
}

func (t *tail) addL(L *lua.LState) int {
	codec := L.CheckString(1)
	switch codec {
	case "json":
		t.cfg.add = newAddJson(L.Get(2))
		goto done
	case "raw":
		t.cfg.add = newAddRaw(L.Get(2))
		goto done
	}

done:
	return t.ret(L)
}

func (t *tail) jsonL(L *lua.LState) int {
	t.cfg.enc = newJson(L.Get(1))
	return t.ret(L)
}

func (t *tail) threadL(L *lua.LState) int {
	n := L.IsInt(1)
	if n >= 1 {
		t.cfg.thread = n
	}
	return t.ret(L)
}

func (t *tail) Index(L *lua.LState, key string) lua.LValue {
	switch key {

	case "pipe":
		return L.NewFunction(t.pipeL)

	case "add":
		return L.NewFunction(t.addL)

	case "thread":
		return L.NewFunction(t.threadL)

	case "json":
		return L.NewFunction(t.jsonL)

	case "file":
		return L.NewFunction(t.fileL)

	case "dir":
		return L.NewFunction(t.dirL)

	case "limit":
		return L.NewFunction(t.limitL)

	case "to":
		return L.NewFunction(t.toL)

	case "start":
		return L.NewFunction(t.startL)

	}

	return lua.LNil
}
