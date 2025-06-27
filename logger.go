package persistence

import (
	"fmt"
	"os"
)

type Logger interface {
	Debug(format string, args ...any)
	Info(format string, args ...any)
	Warn(format string, args ...any)
	Error(format string, args ...any)
	Fatal(format string, args ...any)
}

var LoggerEnabled = false

type defaultLogger struct {
}

func (d *defaultLogger) Debug(format string, args ...any) {
	if LoggerEnabled {
		fmt.Printf("[DEBUG] "+format+"\n", args...)
	}
}

func (d *defaultLogger) Info(format string, args ...any) {
	if LoggerEnabled {
		fmt.Printf("[INFO] "+format+"\n", args...)
	}
}

func (d *defaultLogger) Warn(format string, args ...any) {
	if LoggerEnabled {
		fmt.Printf("[WARN] "+format+"\n", args...)
	}
}

func (d *defaultLogger) Error(format string, args ...any) {
	if LoggerEnabled {
		fmt.Printf("[ERROR] "+format+"\n", args...)
	}
}

func (d *defaultLogger) Fatal(format string, args ...any) {
	if LoggerEnabled {
		fmt.Printf("[FATAL] "+format+"\n", args...)
		os.Exit(1)
	}
}
