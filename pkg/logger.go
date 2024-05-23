package pkg

type Logger interface {
	Info(args ...any)
	Debug(args ...any)
	Error(args ...any)
	Warn(args ...any)
	Panic(args ...any)
	Fatal(args ...any)
}
