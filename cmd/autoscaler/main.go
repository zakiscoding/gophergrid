// Command autoscaler adds worker containers when the queue grows and removes them when it
// drains. It is the Docker Compose counterpart of the KEDA ScaledObject in deploy/k8s.
//
// Every interval it reads /v1/autoscale from a scheduler and computes
//
//	desired = clamp(ceil(queued / TARGET_QUEUE_PER_WORKER), MIN_WORKERS, MAX_WORKERS)
//
// extra workers. It scales up at once and scales down only after the queue has been small
// enough for COOLDOWN, so a burst doesn't make it thrash. New containers clone the
// environment of a template worker (WORKER_ID replaced) and join its network. Removal
// sends SIGTERM, and the worker drains its running jobs before exiting.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zakiscoding/gophergrid/internal/config"
)

const label = "gophergrid.autoscaled"

var (
	desiredG = promauto.NewGauge(prometheus.GaugeOpts{Name: "gophergrid_autoscaler_desired_workers", Help: "Autoscaled workers wanted."})
	currentG = promauto.NewGauge(prometheus.GaugeOpts{Name: "gophergrid_autoscaler_current_workers", Help: "Autoscaled workers running."})
	scaleC   = promauto.NewCounterVec(prometheus.CounterOpts{Name: "gophergrid_autoscaler_scale_events_total", Help: "Scale events."}, []string{"direction"})
)

type pressure struct {
	Queued  int `json:"queued"`
	Running int `json:"running"`
}

func main() {
	log := config.Logger("autoscaler")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	api := config.String("SCHEDULER_URLS", "http://scheduler-1:8080")
	template := config.String("TEMPLATE_CONTAINER", "gophergrid-worker-1-1")
	minW, maxW := int(config.Int("MIN_WORKERS", 0)), int(config.Int("MAX_WORKERS", 6))
	target := float64(config.Int("TARGET_QUEUE_PER_WORKER", 100))
	cooldown := config.Duration("COOLDOWN", 30*time.Second)
	every := config.Duration("INTERVAL", 2*time.Second)

	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		_ = (&http.Server{Addr: config.String("HTTP_ADDR", ":9200"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe()
	}()

	lastHigh := time.Now()
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		p, err := readPressure(ctx, strings.Split(api, ","))
		if err != nil {
			log.Warn("read pressure", "err", err)
			continue
		}
		current, err := list(ctx)
		if err != nil {
			log.Error("list containers", "err", err)
			continue
		}
		want := int(math.Ceil(float64(p.Queued) / target))
		want = max(minW, min(maxW, want))
		desiredG.Set(float64(want))
		currentG.Set(float64(len(current)))
		switch {
		case want > len(current):
			lastHigh = time.Now()
			for i := len(current); i < want; i++ {
				name, err := create(ctx, template, current)
				if err != nil {
					log.Error("scale up", "err", err)
					break
				}
				current = append(current, name)
				scaleC.WithLabelValues("up").Inc()
				log.Info("scaled up", "container", name, "queued", p.Queued, "workers", len(current))
			}
		case want < len(current) && time.Since(lastHigh) > cooldown:
			sort.Strings(current)
			victim := current[len(current)-1]
			if err := remove(ctx, victim); err != nil {
				log.Error("scale down", "err", err)
				continue
			}
			scaleC.WithLabelValues("down").Inc()
			log.Info("scaled down", "container", victim, "queued", p.Queued, "workers", len(current)-1)
		case want >= len(current):
			lastHigh = time.Now()
		}
	}
}

func readPressure(ctx context.Context, urls []string) (pressure, error) {
	var last error
	for _, u := range urls {
		req, _ := http.NewRequestWithContext(ctx, "GET", strings.TrimSpace(u)+"/v1/autoscale", nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			last = err
			continue
		}
		var p pressure
		err = json.NewDecoder(resp.Body).Decode(&p)
		resp.Body.Close()
		if err == nil {
			return p, nil
		}
		last = err
	}
	return pressure{}, last
}

// --- Docker Engine API -----------------------------------------------------------------

var dc = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, "unix", "/var/run/docker.sock")
}}}

func do(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(ctx, method, "http://docker"+path, r)
	req.Header.Set("Content-Type", "application/json")
	resp, err := dc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotModified {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return fmt.Errorf("docker %s %s: %s: %s", method, path, resp.Status, msg)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func list(ctx context.Context) ([]string, error) {
	f, _ := json.Marshal(map[string][]string{"label": {label + "=true"}, "status": {"running", "created", "restarting"}})
	var cs []struct{ Names []string }
	if err := do(ctx, "GET", "/containers/json?all=1&filters="+url.QueryEscape(string(f)), nil, &cs); err != nil {
		return nil, err
	}
	var out []string
	for _, c := range cs {
		if len(c.Names) > 0 {
			out = append(out, strings.TrimPrefix(c.Names[0], "/"))
		}
	}
	return out, nil
}

func create(ctx context.Context, template string, existing []string) (string, error) {
	var tpl struct {
		Config struct {
			Image string
			Env   []string
		}
		NetworkSettings struct{ Networks map[string]any }
	}
	if err := do(ctx, "GET", "/containers/"+template+"/json", nil, &tpl); err != nil {
		return "", err
	}
	taken := map[string]bool{}
	for _, n := range existing {
		taken[n] = true
	}
	name := ""
	for i := 1; ; i++ {
		name = fmt.Sprintf("gophergrid-auto-%02d", i)
		if !taken[name] {
			break
		}
	}
	env := []string{}
	for _, e := range tpl.Config.Env {
		if !strings.HasPrefix(e, "WORKER_ID=") {
			env = append(env, e)
		}
	}
	env = append(env, "WORKER_ID="+name)
	var network string
	for n := range tpl.NetworkSettings.Networks {
		network = n
	}
	body := map[string]any{
		"Image": tpl.Config.Image, "Env": env,
		"Labels":           map[string]string{label: "true"},
		"HostConfig":       map[string]any{"NetworkMode": network, "AutoRemove": true},
		"NetworkingConfig": map[string]any{"EndpointsConfig": map[string]any{network: map[string]any{}}},
	}
	var created struct{ ID string `json:"Id"` }
	_ = do(ctx, "DELETE", "/containers/"+name+"?force=true", nil, nil) // leftover from a crash
	if err := do(ctx, "POST", "/containers/create?name="+name, body, &created); err != nil {
		return "", err
	}
	return name, do(ctx, "POST", "/containers/"+created.ID+"/start", nil, nil)
}

// remove stops a worker with SIGTERM and up to 35 s to drain; AutoRemove deletes it.
func remove(ctx context.Context, name string) error {
	return do(ctx, "POST", "/containers/"+name+"/stop?t=35", nil, nil)
}
