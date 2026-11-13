package worker

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// DiscoverGPUs returns the GPUs for a mode:
//   - "none": no GPUs
//   - "sim": n fake GPUs with memMB each, for running GPU placement without hardware
//   - "nvidia": real GPUs from nvidia-smi (needs the NVIDIA container toolkit in Docker)
func DiscoverGPUs(ctx context.Context, mode, workerID string, n int, memMB int64, model string) ([]GPU, error) {
	switch mode {
	case "", "none":
		return nil, nil
	case "sim":
		gpus := make([]GPU, n)
		for i := range gpus {
			// Unique across workers, like real GPU UUIDs.
			gpus[i] = GPU{ID: workerID + "-gpu" + strconv.Itoa(i), Model: model, MemoryMB: memMB}
		}
		return gpus, nil
	case "nvidia":
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=uuid,name,memory.total",
			"--format=csv,noheader,nounits").Output()
		if err != nil {
			return nil, fmt.Errorf("nvidia-smi: %w", err)
		}
		return parseNvidiaSMI(string(out))
	default:
		return nil, fmt.Errorf("unknown GPU mode %q (want none, sim or nvidia)", mode)
	}
}

func parseNvidiaSMI(out string) ([]GPU, error) {
	var gpus []GPU
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		f := strings.Split(line, ",")
		if len(f) != 3 {
			return nil, fmt.Errorf("unexpected nvidia-smi line %q", line)
		}
		mem, err := strconv.ParseInt(strings.TrimSpace(f[2]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bad memory in %q: %w", line, err)
		}
		gpus = append(gpus, GPU{ID: strings.TrimSpace(f[0]), Model: strings.TrimSpace(f[1]), MemoryMB: mem})
	}
	return gpus, nil
}
