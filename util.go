package tail

import (
	"github.com/rock-go/rock/json"
	"github.com/rock-go/rock/lua"
	"github.com/rock-go/rock/lua/grep"
)

func compile(v string) func(string) bool {
	m := func(_ string) bool { return true }
	if v == "*" {
		return m
	}

	g, err := grep.Compile(v, nil)
	if err != nil {
		m = func(_ string) bool { return false }
	} else {
		m = g.Match
	}
	return m
}

func newJson(val lua.LValue) func([]byte) []byte {
	switch val.Type() {
	case lua.LTTable:
		ex := val.(*lua.LTable)
		return func(v []byte) []byte {
			buf := json.NewEncoder()
			buf.Tab("")
			ex.Range(func(key string, val lua.LValue) {
				buf.KV(key, val.String())
			})
			buf.KV("msg", v)
			buf.End("}")
			return buf.Bytes()
		}

	default:
		return nil
	}
}

func newAddJson(val lua.LValue) func([]byte) []byte {

	switch val.Type() {
	case lua.LTString:
		return func(v []byte) []byte {
			n := len(v)
			ch := v[n-1]
			v[n-1] = ','
			v = append(v, lua.S2B(val.String())...)
			v = append(v, ch)
			return v
		}

	case lua.LTTable:
		ex := val.(*lua.LTable)
		return func(v []byte) []byte {
			n := len(v)
			ch := v[n-1]
			v[n-1] = ','
			buff := json.New(v)
			ex.Range(func(key string, val lua.LValue) {
				buff.KV(key, val.String())
			})
			v = append(v, ch)
			return v
		}

	default:
		return nil
	}

}

func newAddRaw(val lua.LValue) func([]byte) []byte {
	switch val.Type() {
	case lua.LTString:
		return func(v []byte) []byte {
			return append(v, lua.S2B(val.String())...)
		}

	default:
		return nil
	}
}
