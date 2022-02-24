package tail

import (
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/xbase"
	"reflect"
)

var (
	xEnv   *xbase.EnvT
	typeof = reflect.TypeOf((*tail)(nil)).String()
)

/*
	local t = rock.tail("error")

	t.limit(10)
	t.json{"a"=1,"b"=2 , "c" = 3}
	t.buffer(100)
	t.to(k)
	t.pipe(_(raw) print(raw) end)
	t.start()

	local dx = t.dir("/var/log" , "*.log")
	dx.on(_(fx)
		fx.poll(100)
		fx.inotify()
		fx.delim('\n')
		fx.buffer(4096)
		fx.bkt("tail_seed_record")
		fx.pipe($(raw) print(raw) end)
		fx.to(k)
		fx.run()
    end)

	local fx = t.file("/var/log/x.log")
	fx.poll(100)
	fx.inotify()
	fx.delim('\n')
	fx.buffer(4096)
	fx.bkt("tail_seed_record")
	fx.pipe($(raw) print(raw) end)
	fx.to(k)
	fx.run()


*/
func newLuaTail(L *lua.LState) int {
	cfg := newConfig(L)

	proc := L.NewProc(cfg.name, typeof)
	if proc.IsNil() {
		proc.Set(newTail(cfg))
	} else {
		t := proc.Data.(*tail)
		t.cfg = cfg
	}

	L.Push(proc)
	return 1
}

func LuaInjectApi(env *xbase.EnvT) {
	xEnv = env
	xEnv.Set("tail", lua.NewFunction(newLuaTail))
}
