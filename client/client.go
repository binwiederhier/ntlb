// Package client talks to a running ntfy-lb's admin API, for the CLI
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"heckel.io/ntfy-lb/balancer"
	"heckel.io/ntfy-lb/server"
)

const (
	requestTimeout = 30 * time.Second // A reload probes new upstreams, which takes up to the health-check timeout
)

// Client calls the admin API at a base URL, e.g. http://127.0.0.1:2590
type Client struct {
	baseURL string
	http    *http.Client
}

// New creates a client for the admin API at baseURL
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimSuffix(baseURL, "/"),
		http: &http.Client{
			Timeout: requestTimeout,
		},
	}
}

// Status returns the load balancer's status
func (c *Client) Status(ctx context.Context) (*balancer.Status, error) {
	status := &balancer.Status{}
	if err := c.do(ctx, http.MethodGet, "/v1/status", nil, status); err != nil {
		return nil, err
	}
	return status, nil
}

// Reload makes the load balancer re-read its config file. It returns the changed settings that
// only a restart applies.
func (c *Client) Reload(ctx context.Context) ([]string, error) {
	resp := &server.ReloadResponse{}
	if err := c.do(ctx, http.MethodPost, "/v1/reload", nil, resp); err != nil {
		return nil, err
	}
	return resp.RestartRequired, nil
}

// Drain drains the load balancer over duration, or the config's drain-duration if empty
func (c *Client) Drain(ctx context.Context, duration string) error {
	return c.do(ctx, http.MethodPost, "/v1/drain", &server.DrainRequest{Duration: duration}, nil)
}

// Undrain ends a drain of the load balancer
func (c *Client) Undrain(ctx context.Context) error {
	return c.do(ctx, http.MethodDelete, "/v1/drain", nil, nil)
}

// DrainUpstream drains one upstream over duration, or the config's drain-duration if empty
func (c *Client) DrainUpstream(ctx context.Context, name, duration string) error {
	return c.do(ctx, http.MethodPost, "/v1/upstreams/"+url.PathEscape(name)+"/drain", &server.DrainRequest{Duration: duration}, nil)
}

// UndrainUpstream puts a drained upstream back into rotation
func (c *Client) UndrainUpstream(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, "/v1/upstreams/"+url.PathEscape(name)+"/drain", nil, nil)
}

func (c *Client) do(ctx context.Context, method, path string, body, result any) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reqBody)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach ntfy-lb admin API: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		errResp := &server.ErrorResponse{}
		if err := json.Unmarshal(b, errResp); err == nil && errResp.Error != "" {
			return errors.New(errResp.Error)
		}
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	if result != nil {
		return json.Unmarshal(b, result)
	}
	return nil
}
