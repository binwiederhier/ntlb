package client

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"heckel.io/ntlb/balancer"
)

// recordedRequest is what the fake admin API saw
type recordedRequest struct {
	method string
	uri    string
	body   string
}

// newFakeAdmin runs an admin API that records the request and answers with status and body
func newFakeAdmin(t *testing.T, status int, body string) (*Client, *recordedRequest) {
	rec := &recordedRequest{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		rec.method = r.Method
		rec.uri = r.RequestURI
		rec.body = string(b)
		w.WriteHeader(status)
		io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL + "/"), rec // Trailing slash is trimmed
}

// Success paths, one per method

func TestClient_Status(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"healthy":true,"ready":true,"streams":3,"upstreams":[{"name":"app1","state":"healthy","addresses":["10.0.0.1:80"]}]}`)
	status, err := c.Status(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.MethodGet, rec.method)
	require.Equal(t, "/v1/status", rec.uri)
	require.Empty(t, rec.body)
	require.True(t, status.Ready)
	require.Equal(t, 3, status.Streams)
	require.Len(t, status.Upstreams, 1)
	require.Equal(t, "app1", status.Upstreams[0].Name)
	require.Equal(t, balancer.StateHealthy, status.Upstreams[0].State)
}

func TestClient_Status_InvalidJSON(t *testing.T) {
	c, _ := newFakeAdmin(t, http.StatusOK, `not json`)
	_, err := c.Status(context.Background())
	require.Error(t, err)
}

func TestClient_Reload(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	restart, err := c.Reload(context.Background())
	require.NoError(t, err)
	require.Empty(t, restart)
	require.Equal(t, http.MethodPost, rec.method)
	require.Equal(t, "/v1/reload", rec.uri)
	require.Empty(t, rec.body)
}

func TestClient_Reload_RestartRequired(t *testing.T) {
	c, _ := newFakeAdmin(t, http.StatusOK, `{"success":true,"restart_required":["listen-http","client-keepalive"]}`)
	restart, err := c.Reload(context.Background())
	require.NoError(t, err)
	require.Equal(t, []string{"listen-http", "client-keepalive"}, restart)
}

func TestClient_Drain(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	require.NoError(t, c.Drain(context.Background(), "10m"))
	require.Equal(t, http.MethodPost, rec.method)
	require.Equal(t, "/v1/drain", rec.uri)
	require.JSONEq(t, `{"duration":"10m"}`, rec.body)
}

func TestClient_Drain_DefaultDuration(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	require.NoError(t, c.Drain(context.Background(), ""))
	require.JSONEq(t, `{}`, rec.body)
}

func TestClient_Undrain(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	require.NoError(t, c.Undrain(context.Background()))
	require.Equal(t, http.MethodDelete, rec.method)
	require.Equal(t, "/v1/drain", rec.uri)
	require.Empty(t, rec.body)
}

func TestClient_DrainUpstream(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	require.NoError(t, c.DrainUpstream(context.Background(), "app1", "30s"))
	require.Equal(t, http.MethodPost, rec.method)
	require.Equal(t, "/v1/upstreams/app1/drain", rec.uri)
	require.JSONEq(t, `{"duration":"30s"}`, rec.body)
}

func TestClient_UndrainUpstream(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	require.NoError(t, c.UndrainUpstream(context.Background(), "app1"))
	require.Equal(t, http.MethodDelete, rec.method)
	require.Equal(t, "/v1/upstreams/app1/drain", rec.uri)
	require.Empty(t, rec.body)
}

func TestClient_UpstreamNameEscaped(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	require.NoError(t, c.DrainUpstream(context.Background(), "a/b c?d", ""))
	require.Equal(t, "/v1/upstreams/a%2Fb%20c%3Fd/drain", rec.uri)
	require.NoError(t, c.UndrainUpstream(context.Background(), "a/b"))
	require.Equal(t, "/v1/upstreams/a%2Fb/drain", rec.uri)
}

// Errors

func TestClient_ErrorJSON(t *testing.T) {
	c, _ := newFakeAdmin(t, http.StatusNotFound, `{"error":"upstream not found"}`)
	err := c.DrainUpstream(context.Background(), "nope", "")
	require.EqualError(t, err, "upstream not found")
	_, err = c.Reload(context.Background())
	require.EqualError(t, err, "upstream not found")
	_, err = c.Status(context.Background())
	require.EqualError(t, err, "upstream not found")
	require.EqualError(t, c.Drain(context.Background(), ""), "upstream not found")
	require.EqualError(t, c.Undrain(context.Background()), "upstream not found")
	require.EqualError(t, c.UndrainUpstream(context.Background(), "nope"), "upstream not found")
}

func TestClient_ErrorNotJSON(t *testing.T) {
	c, _ := newFakeAdmin(t, http.StatusBadGateway, "bad gateway\n")
	err := c.Undrain(context.Background())
	require.EqualError(t, err, "unexpected status 502: bad gateway")
}

func TestClient_ErrorJSONWithoutMessage(t *testing.T) {
	c, _ := newFakeAdmin(t, http.StatusInternalServerError, `{"success":false}`)
	err := c.Undrain(context.Background())
	require.EqualError(t, err, `unexpected status 500: {"success":false}`)
}

func TestClient_ErrorEmptyBody(t *testing.T) {
	c, _ := newFakeAdmin(t, http.StatusServiceUnavailable, "")
	_, err := c.Status(context.Background())
	require.EqualError(t, err, "unexpected status 503: ")
}

func TestClient_NonOKSuccessStatus(t *testing.T) {
	// Only 200 counts as success, even a 2xx with a valid body
	c, _ := newFakeAdmin(t, http.StatusAccepted, `{"success":true}`)
	err := c.Undrain(context.Background())
	require.EqualError(t, err, `unexpected status 202: {"success":true}`)
}

func TestClient_Unreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	c := New("http://" + addr)
	_, err = c.Status(context.Background())
	require.ErrorContains(t, err, "cannot reach ntlb admin API")
	var opErr *net.OpError
	require.ErrorAs(t, err, &opErr)
}

func TestClient_InvalidURL(t *testing.T) {
	c := New("http://[::1")
	_, err := c.Status(context.Background())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "cannot reach")
}

func TestClient_ContextCanceled(t *testing.T) {
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	errChan := make(chan error, 1)
	go func() {
		_, err := New(srv.URL).Status(ctx)
		errChan <- err
	}()
	<-started
	cancel()
	select {
	case err := <-errChan:
		require.ErrorContains(t, err, "cannot reach ntlb admin API")
		require.True(t, errors.Is(err, context.Canceled))
	case <-time.After(5 * time.Second):
		t.Fatal("request not canceled")
	}
}

func TestClient_ContextCanceledBefore(t *testing.T) {
	c, rec := newFakeAdmin(t, http.StatusOK, `{"success":true}`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := c.Undrain(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, rec.method)
}
