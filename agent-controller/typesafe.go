package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// typesafeClient talks to the TypeSafe System One evaluation endpoint. Jev
// answers typed questions against a state; it never generates free text, so
// every answer is a decision this code can branch on directly. Candidates,
// channels and thresholds are enumerated here, which structurally prevents the
// model from inventing recipients or destinations.
type typesafeClient struct {
	endpoint string
	apiKey   string
	model    string
	timeout  time.Duration
	client   *http.Client
}

// newTypesafeClient returns nil unless the API key is configured; the chat
// decision pipeline then stays disabled and chat falls back to the ordinary
// decision model exactly as before.
func newTypesafeClient(cfg config) *typesafeClient {
	if cfg.typesafeAPIKey == "" || cfg.typesafeEndpoint == "" || cfg.typesafeModel == "" {
		return nil
	}
	return &typesafeClient{
		endpoint: cfg.typesafeEndpoint,
		apiKey:   cfg.typesafeAPIKey,
		model:    cfg.typesafeModel,
		timeout:  cfg.chatRequestTimeout,
		client:   &http.Client{Timeout: cfg.chatRequestTimeout},
	}
}

type typesafeQuestion struct {
	Type         string `json:"type"` // "noul" | "choice" | "score"
	Instructions any    `json:"instructions"`
	Criteria     any    `json:"criteria,omitempty"`
}

type typesafeAnswer struct {
	Type          string             `json:"type"`
	Noul          float64            `json:"noul"`
	Choice        string             `json:"choice"`
	Confidence    float64            `json:"confidence"`
	Probabilities map[string]float64 `json:"probabilities"`
}

// evaluate sends one state with all questions in a single request; TypeSafe
// evaluates them in parallel and returns one typed answer per question id.
// Rate-limit and overload responses are retried with backoff per the API
// contract; every other failure fails closed so a silent bot is preferred
// over an unvalidated one.
func (client *typesafeClient) evaluate(state any, questions map[string]typesafeQuestion) (map[string]typesafeAnswer, error) {
	body, err := json.Marshal(map[string]any{"state": state, "model": client.model, "questions": questions})
	if err != nil {
		return nil, fmt.Errorf("encode typesafe request: %w", err)
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		answers, retryable, err := client.attempt(body)
		if err == nil {
			return answers, nil
		}
		lastErr = err
		if !retryable {
			return nil, err
		}
	}
	return nil, lastErr
}

func (client *typesafeClient) attempt(body []byte) (map[string]typesafeAnswer, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, false, fmt.Errorf("create typesafe request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+client.apiKey)
	resp, err := client.client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("typesafe request failed: %w", err)
	}
	defer resp.Body.Close()
	responseBytes, err := io.ReadAll(io.LimitReader(resp.Body, maxLLMResponseBytes+1))
	if err != nil {
		return nil, false, fmt.Errorf("read typesafe response: %w", err)
	}
	if len(responseBytes) > maxLLMResponseBytes {
		return nil, false, fmt.Errorf("typesafe response exceeded size limit")
	}
	if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == 529 {
		return nil, true, fmt.Errorf("typesafe returned HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("typesafe returned HTTP %d", resp.StatusCode)
	}
	var decoded struct {
		Answers map[string]typesafeAnswer `json:"answers"`
	}
	if err := json.Unmarshal(responseBytes, &decoded); err != nil {
		return nil, false, fmt.Errorf("decode typesafe response: %w", err)
	}
	if len(decoded.Answers) == 0 {
		return nil, false, fmt.Errorf("typesafe response carried no answers")
	}
	return decoded.Answers, false, nil
}
