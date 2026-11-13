// Package config reads settings from environment variables.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns the variable or def.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns the variable parsed as an int, or def. It exits on a malformed value, since
// running with a silently ignored setting is worse than not starting.
func Int(key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		fatal(key, v, err)
	}
	return n
}

// Duration returns the variable parsed as a Go duration (e.g. 500ms, 10s), or def.
func Duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		fatal(key, v, err)
	}
	return d
}

// Bool returns the variable parsed as a bool, or def.
func Bool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		fatal(key, v, err)
	}
	return b
}

// List splits a comma-separated variable.
func List(key, def string) []string {
	var out []string
	for _, s := range strings.Split(String(key, def), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Logger returns a JSON logger at LOG_LEVEL (debug, info, warn, error).
func Logger(component string) *slog.Logger {
	var level slog.Level
	if err := level.UnmarshalText([]byte(String("LOG_LEVEL", "info"))); err != nil {
		level = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})).With("component", component)
}

func fatal(key, v string, err error) {
	fmt.Fprintf(os.Stderr, "config: %s=%q: %v\n", key, v, err)
	os.Exit(2)
}
