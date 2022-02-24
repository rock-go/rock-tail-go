package tail

type line struct {
	value []byte
	enc   func([]byte) []byte
	add   func([]byte) []byte
}

func (l *line) byte() []byte {
	if l.add != nil {
		l.value = l.add(l.value)
	}

	if l.enc != nil {
		l.value = l.enc(l.value)
	}

	return l.value
}

func (l *line) Enc(fn func([]byte) []byte) {
	if l.enc != nil {
		return
	}

	l.enc = fn
}

func (l *line) Add(fn func([]byte) []byte) {
	if l.add != nil {
		return
	}
	l.add = fn
}
