package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/zakiscoding/gophergrid/internal/pullapi"
)

// HTTPTransport is the phase 1 transport: claim jobs over HTTP. It has no heartbeats, so a
// dead worker's jobs are only retried when their lease (timeout + grace) runs out.
type HTTPTransport struct {
	BaseURL  string
	WorkerID string
	Client   *http.Client
}

// Register is a no-op in phase 1: the scheduler doesn't track workers yet.
func (t *HTTPTransport) Register(context.Context, Info) error { return nil }

// Sync claims as many jobs as fit in the free resources.
func (t *HTTPTransport) Sync(ctx context.Context, st State) (SyncResult, error) {
	if st.Draining || st.Free.CPUMillis <= 0 || st.Free.MemoryMB <= 0 {
		return SyncResult{}, nil
	}
	var resp struct {
		Tasks []pullapi.Task `json:"tasks"`
	}
	err := t.post(ctx, "/internal/v1/claim", pullapi.ClaimRequest{WorkerID: t.WorkerID, Free: st.Free, Max: 8}, &resp)
	if err != nil {
		return SyncResult{}, err
	}
	var res SyncResult
	for _, pt := range resp.Tasks {
		res.Tasks = append(res.Tasks, Task{JobID: pt.JobID, Attempt: pt.Attempt, Type: pt.Type, Payload: pt.Payload,
			Resources: pt.Resources, Timeout: time.Duration(pt.TimeoutMS) * time.Millisecond})
	}
	return res, nil
}

// Complete reports a result.
func (t *HTTPTransport) Complete(ctx context.Context, r Result) (bool, error) {
	var resp struct {
		Accepted bool `json:"accepted"`
	}
	err := t.post(ctx, "/internal/v1/complete", pullapi.CompleteRequest{WorkerID: t.WorkerID, JobID: r.JobID,
		Attempt: r.Attempt, Success: r.Success, Permanent: r.Permanent, Output: r.Output, Error: r.Error,
		DurationMS: r.Duration.Milliseconds()}, &resp)
	return resp.Accepted, err
}

func (t *HTTPTransport) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.BaseURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("%s: %s: %s", path, resp.Status, bytes.TrimSpace(msg))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
