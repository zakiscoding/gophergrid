// Command fakellm is a deterministic stand-in for vLLM's OpenAI-compatible chat API, used by
// the integration tests to run `llm` jobs (and kill workers mid-request) without a GPU.
//
// The reply depends only on the model, prompt, seed and max_tokens, like greedy decoding on
// a real server, and every request takes FAKE_LATENCY plus FAKE_TOKEN_LATENCY per max_tokens. GET /stats counts requests, so a test
// can see that a job ran more than once while recording one result.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

func main() {
	latency, err := time.ParseDuration(env("FAKE_LATENCY", "200ms"))
	if err != nil {
		log.Fatal(err)
	}
	perToken, err := time.ParseDuration(env("FAKE_TOKEN_LATENCY", "0s"))
	if err != nil {
		log.Fatal(err)
	}
	key := os.Getenv("FAKE_API_KEY")
	var requests atomic.Int64

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]int64{"requests": requests.Load()})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if key != "" && r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, `{"error":"bad key"}`, http.StatusUnauthorized)
			return
		}
		var req struct {
			Model     string `json:"model"`
			MaxTokens int    `json:"max_tokens"`
			Seed      int64  `json:"seed"`
			Messages  []struct {
				Role, Content string
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Messages) == 0 || req.MaxTokens < 1 {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		requests.Add(1)
		select {
		case <-time.After(latency + time.Duration(req.MaxTokens)*perToken):
		case <-r.Context().Done():
			return
		}
		prompt := req.Messages[len(req.Messages)-1].Content
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d|%s", req.Model, req.Seed, req.MaxTokens, prompt)))
		words := make([]string, min(req.MaxTokens, 16))
		for i := range words {
			words[i] = hex.EncodeToString(sum[i%32 : i%32+1])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"model": req.Model,
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "length",
				"message": map[string]string{"role": "assistant", "content": strings.Join(words, " ")},
			}},
			"usage": map[string]int{"prompt_tokens": len(strings.Fields(prompt)), "completion_tokens": len(words)},
		})
	})
	addr := env("ADDR", ":8000")
	log.Printf("fakellm on %s, latency %s", addr, latency)
	log.Fatal((&http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
