package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"math/rand"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/zakiscoding/gophergrid/internal/jobs"
)

// Task is one attempt of a job handed to a runner.
type Task struct {
	JobID     string
	Attempt   int
	Type      string
	Payload   json.RawMessage
	Resources jobs.Resources
	GPUIDs    []string
	Timeout   time.Duration
}

// Runner executes one job type. Run must return when ctx is cancelled.
type Runner func(ctx context.Context, t Task) (json.RawMessage, error)

// PermanentError marks a failure that retrying won't fix (bad payload, unknown type).
type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

func permanent(format string, a ...any) error { return PermanentError{fmt.Errorf(format, a...)} }

// Runners returns the built-in runners. The container runner is only included when
// enableContainers is true, because it needs a Docker socket.
func Runners(enableContainers bool, gpuMode string) map[string]Runner {
	r := map[string]Runner{
		jobs.TypeSleep:     runSleep,
		jobs.TypeImage:     runImage,
		jobs.TypeHash:      runHash,
		jobs.TypeInference: runInferenceSim,
	}
	if enableContainers {
		r[jobs.TypeContainer] = containerRunner(gpuMode)
	}
	return r
}

func decode(t Task, v any) error {
	if len(t.Payload) == 0 {
		return nil
	}
	if err := json.Unmarshal(t.Payload, v); err != nil {
		return permanent("bad %s payload: %v", t.Type, err)
	}
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-tm.C:
		return nil
	}
}

func runSleep(ctx context.Context, t Task) (json.RawMessage, error) {
	var p struct {
		MS     int64 `json:"ms"`
		FailOn int   `json:"fail_attempts"` // fail this many attempts first (for retry tests)
	}
	if err := decode(t, &p); err != nil {
		return nil, err
	}
	if p.MS < 0 || p.MS > 3_600_000 {
		return nil, permanent("ms out of range")
	}
	if err := sleepCtx(ctx, time.Duration(p.MS)*time.Millisecond); err != nil {
		return nil, err
	}
	if t.Attempt <= p.FailOn {
		return nil, fmt.Errorf("injected failure on attempt %d", t.Attempt)
	}
	return json.Marshal(map[string]any{"slept_ms": p.MS})
}

// runImage is the "image-processing job" from the spec: it renders a deterministic noise
// image, applies a box blur and a grayscale pass, and returns a checksum of the pixels, so
// two attempts of the same job always produce the same result.
func runImage(ctx context.Context, t Task) (json.RawMessage, error) {
	p := struct {
		Width  int   `json:"width"`
		Height int   `json:"height"`
		Radius int   `json:"blur_radius"`
		Seed   int64 `json:"seed"`
	}{Width: 256, Height: 256, Radius: 2, Seed: 1}
	if err := decode(t, &p); err != nil {
		return nil, err
	}
	if p.Width < 1 || p.Height < 1 || p.Width*p.Height > 16_000_000 || p.Radius < 0 || p.Radius > 32 {
		return nil, permanent("image size or blur radius out of range")
	}
	src := image.NewRGBA(image.Rect(0, 0, p.Width, p.Height))
	rng := rand.New(rand.NewSource(p.Seed))
	rng.Read(src.Pix)
	gray := image.NewGray(src.Rect)
	r := p.Radius
	for y := 0; y < p.Height; y++ {
		if y%64 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		for x := 0; x < p.Width; x++ {
			var sr, sg, sb, n int
			for dy := -r; dy <= r; dy++ {
				yy := y + dy
				if yy < 0 || yy >= p.Height {
					continue
				}
				for dx := -r; dx <= r; dx++ {
					xx := x + dx
					if xx < 0 || xx >= p.Width {
						continue
					}
					c := src.RGBAAt(xx, yy)
					sr, sg, sb, n = sr+int(c.R), sg+int(c.G), sb+int(c.B), n+1
				}
			}
			// ITU-R BT.601 luma
			l := (299*sr + 587*sg + 114*sb) / (1000 * n)
			gray.SetGray(x, y, color.Gray{Y: uint8(l)})
		}
	}
	sum := sha256.Sum256(gray.Pix)
	return json.Marshal(map[string]any{
		"width": p.Width, "height": p.Height, "blur_radius": p.Radius,
		"checksum": hex.EncodeToString(sum[:8]),
	})
}

func runHash(ctx context.Context, t Task) (json.RawMessage, error) {
	p := struct {
		Rounds int    `json:"rounds"`
		Input  string `json:"input"`
	}{Rounds: 100_000, Input: t.JobID}
	if err := decode(t, &p); err != nil {
		return nil, err
	}
	if p.Rounds < 1 || p.Rounds > 50_000_000 {
		return nil, permanent("rounds out of range")
	}
	h := sha256.Sum256([]byte(p.Input))
	for i := 1; i < p.Rounds; i++ {
		if i%10_000 == 0 && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		h = sha256.Sum256(h[:])
	}
	return json.Marshal(map[string]any{"rounds": p.Rounds, "digest": hex.EncodeToString(h[:])})
}

// runInferenceSim stands in for a batched LLM inference shard on a simulated GPU. Time is
// prompts*tokens*ms_per_token, divided across the GPUs the scheduler assigned.
func runInferenceSim(ctx context.Context, t Task) (json.RawMessage, error) {
	p := struct {
		Model      string  `json:"model"`
		Prompts    int     `json:"prompts"`
		Tokens     int     `json:"tokens"`
		MSPerToken float64 `json:"ms_per_token"`
	}{Model: "qwen2.5-1.5b-instruct", Prompts: 8, Tokens: 64, MSPerToken: 0.25}
	if err := decode(t, &p); err != nil {
		return nil, err
	}
	if len(t.GPUIDs) == 0 {
		return nil, permanent("inference task was placed without a GPU")
	}
	if p.Prompts < 1 || p.Tokens < 1 || p.Prompts*p.Tokens > 10_000_000 || p.MSPerToken < 0 {
		return nil, permanent("prompts/tokens out of range")
	}
	d := time.Duration(float64(p.Prompts*p.Tokens) * p.MSPerToken / float64(len(t.GPUIDs)) * float64(time.Millisecond))
	if err := sleepCtx(ctx, d); err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{
		"model": p.Model, "prompts": p.Prompts, "generated_tokens": p.Prompts * p.Tokens,
		"gpu_ids": t.GPUIDs, "simulated": true,
	})
}

// containerRunner runs a job as `docker run`, with the job's CPU and memory as hard limits
// and its assigned GPUs passed through when the worker has real NVIDIA GPUs.
func containerRunner(gpuMode string) Runner {
	return func(ctx context.Context, t Task) (json.RawMessage, error) {
		var p struct {
			Image      string   `json:"image"`
			Entrypoint string   `json:"entrypoint"` // optional, overrides the image's
			Cmd        []string `json:"cmd"`
		}
		if err := decode(t, &p); err != nil {
			return nil, err
		}
		if p.Image == "" {
			return nil, permanent("container payload needs image")
		}
		args := []string{"run", "--rm", "--network=none",
			"--name", "gg-" + t.JobID[:8] + "-" + strconv.Itoa(t.Attempt),
			"--cpus", strconv.FormatFloat(float64(t.Resources.CPUMillis)/1000, 'f', 3, 64),
			"--memory", strconv.FormatInt(t.Resources.MemoryMB, 10) + "m",
			"--label", "gophergrid.job=" + t.JobID,
		}
		if len(t.GPUIDs) > 0 && gpuMode == "nvidia" {
			args = append(args, "--gpus", `"device=`+strings.Join(t.GPUIDs, ",")+`"`)
		}
		if p.Entrypoint != "" {
			args = append(args, "--entrypoint", p.Entrypoint)
		}
		args = append(args, p.Image)
		args = append(args, p.Cmd...)
		var out, errb bytes.Buffer
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Stdout, cmd.Stderr = &limited{&out, 64 << 10}, &limited{&errb, 16 << 10}
		if err := cmd.Run(); err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, fmt.Errorf("docker run: %v: %s", err, strings.TrimSpace(errb.String()))
		}
		return json.Marshal(map[string]any{"stdout": out.String()})
	}
}

// limited keeps the first n bytes and drops the rest.
type limited struct {
	b *bytes.Buffer
	n int
}

func (l *limited) Write(p []byte) (int, error) {
	if room := l.n - l.b.Len(); room > 0 {
		if len(p) > room {
			l.b.Write(p[:room])
		} else {
			l.b.Write(p)
		}
	}
	return len(p), nil
}
