package tail

import (
	"fmt"
	"github.com/rock-go/rock/auxlib"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/node"
	"github.com/rock-go/rock/pipe"
	"time"
)

func (fx *Fx) String() string                         { return fmt.Sprintf("%p", &fx) }
func (fx *Fx) Type() lua.LValueType                   { return lua.LTObject }
func (fx *Fx) AssertFloat64() (float64, bool)         { return 0, false }
func (fx *Fx) AssertString() (string, bool)           { return "", false }
func (fx *Fx) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (fx *Fx) Peek() lua.LValue                       { return fx }

func (fx *Fx) waitL(L *lua.LState) int {
	n := L.IsInt(1)
	if n <= 0 {
		fx.wait = 5 * 1000 * time.Millisecond
	} else {
		fx.wait = time.Duration(n) * time.Millisecond
	}

	return 0
}

func (fx *Fx) delimL(L *lua.LState) int {
	val := L.IsString(1)
	if len(val) > 0 {
		fx.delim = val[0]
	}
	return 0
}

func (fx *Fx) bufferL(L *lua.LState) int {
	buffer := L.IsInt(1)
	if buffer > 1 {
		fx.buffer = buffer
	}
	return 0
}

func (fx *Fx) bucketL(L *lua.LState) int {
	n := L.GetTop()
	if n == 0 {
		return 0
	}
	var bkt []string
	for i := 1; i <= n; i++ {
		name := L.IsString(i)
		if len(name) < 2 {
			xEnv.Errorf("%s seek record invalid name %s", fx.path, name)
			return 0
		}
		bkt = append(bkt, name)
	}
	fx.bkt = bkt
	return 0
}

func (fx *Fx) pipeL(L *lua.LState) int {
	fx.pipe = pipe.Check(L)
	return 0
}

func (fx *Fx) jsonL(L *lua.LState) int {
	fx.enc = newJson(L.Get(1))
	return 0
}

func (fx *Fx) nodeL(L *lua.LState) int {
	codec := L.CheckString(1)
	switch codec {
	case "json":
		tab := L.CreateTable(0, 2)
		tab.RawSetString("id", lua.S2L(node.ID()))
		tab.RawSetString("inet", lua.S2L(node.LoadAddr()))
		tab.RawSetString("file", lua.S2L(fx.path))
		fx.enc = newJson(tab)

	case "raw":
		fx.enc = func(v []byte) []byte {
			v = append(v, ' ')
			v = append(v, auxlib.S2B(node.ID())...)
			v = append(v, ' ')
			v = append(v, auxlib.S2B(node.LoadAddr())...)
			v = append(v, ' ')
			v = append(v, auxlib.S2B(fx.path)...)
			return v
		}

	default:
		//todo
	}

	return 0
}

func (fx *Fx) addL(L *lua.LState) int {
	codec := L.CheckString(1)
	switch codec {
	case "json":
		fx.add = newAddJson(L.Get(2))
		return 0
	case "raw":
		fx.add = newAddRaw(L.Get(2))
		return 0
	}
	return 0
}

func (fx *Fx) runL(L *lua.LState) int {
	xEnv.Spawn(0, func() {
		if e := fx.open(); e != nil {
			xEnv.Errorf("%x file open error %v", fx.path, e)
			return
		}
		xEnv.Errorf("%s file open succeed", fx.path)
	})

	return 0
}

func (fx *Fx) onL(L *lua.LState) int {
	pipe := pipe.Check(L)
	if len(pipe) == 0 {
		return 0
	}

	fx.on = pipe[0]
	fx.co = xEnv.Clone(L)

	return 0
}

func (fx *Fx) Index(L *lua.LState, key string) lua.LValue {

	switch key {

	case "json":
		return L.NewFunction(fx.jsonL)

	case "add":
		return L.NewFunction(fx.addL)

	case "wait":
		return L.NewFunction(fx.waitL)

	case "delim":
		return L.NewFunction(fx.delimL)

	case "buffer":
		return L.NewFunction(fx.bufferL)

	case "bkt":
		return L.NewFunction(fx.bucketL)

	case "node":
		return L.NewFunction(fx.nodeL)

	case "pipe":
		return L.NewFunction(fx.pipeL)

	case "run":
		return L.NewFunction(fx.runL)

	case "on":
		return L.NewFunction(fx.onL)

	case "path":
		return lua.S2L(fx.path)

	}

	return lua.LNil
}
