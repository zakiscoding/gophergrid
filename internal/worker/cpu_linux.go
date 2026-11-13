package worker

import (
	"syscall"
	"time"
)

// cpuMeter measures this process's CPU time between samples.
type cpuMeter struct {
	lastCPU  time.Duration
	lastWall time.Time
}

// sample returns CPU used since the last sample as a fraction of capacity (millicores).
func (m *cpuMeter) sample(capacityMillis int64) float64 {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	cpu := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	now := time.Now()
	defer func() { m.lastCPU, m.lastWall = cpu, now }()
	if m.lastWall.IsZero() || capacityMillis <= 0 {
		return 0
	}
	wall := now.Sub(m.lastWall)
	if wall <= 0 {
		return 0
	}
	return min(1, float64(cpu-m.lastCPU)/float64(wall)/(float64(capacityMillis)/1000))
}
