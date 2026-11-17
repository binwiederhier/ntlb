// Package client talks to a running ntlb's admin API, for the CLI
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"heckel.io/ntlb/api"
)

const (
	requestTimeout = 30 * time.Second // A reload probes new upstreams, which takes up to the healthcheck timeout
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
func (c *Client) Status(ctx context.Context) (*api.Status, error) {
	status := &api.Status{}
	if err := c.do(ctx, http.MethodGet, api.StatusPath, nil, status); err != nil {
		return nil, err
	}
	return status, nil
}

// Reload makes the load balancer re-read its config file. It returns the changed settings that
// only a restart applies.
func (c *Client) Reload(ctx context.Context) ([]string, error) {
	resp := &api.ReloadResponse{}
	if err := c.do(ctx, http.MethodPost, api.ReloadPath, nil, resp); err != nil {
		return nil, err
	}
	return resp.RestartRequired, nil
}

// DrainUpstream drains one upstream over duration, or the config's drain-duration if empty
func (c *Client) DrainUpstream(ctx context.Context, name, duration string) error {
	return c.do(ctx, http.MethodPost, api.UpstreamDrainURL(name), &api.DrainRequest{Duration: duration}, nil)
}

// UndrainUpstream puts a drained upstream back into rotation
func (c *Client) UndrainUpstream(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodDelete, api.UpstreamDrainURL(name), nil, nil)
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
		return fmt.Errorf("cannot reach ntlb admin API: %w", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		errResp := &api.ErrorResponse{}
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
