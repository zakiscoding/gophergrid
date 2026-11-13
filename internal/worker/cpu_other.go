//go:build !linux

package worker

type cpuMeter struct{}

func (m *cpuMeter) sample(int64) float64 { return 0 }
