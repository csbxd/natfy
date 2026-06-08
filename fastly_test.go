package natfy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
)

func TestSyncClonesUpdatesValidatesActivatesThenCaches(t *testing.T) {
	var clones atomic.Int32
	var updates atomic.Int32
	var activates atomic.Int32

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Fastly-Key") != "token" {
			t.Fatalf("bad auth header")
		}
		switch r.URL.Path {
		case "/service/svc":
			_, _ = w.Write([]byte(`{"versions":[{"number":3,"active":true}]}`))
		case "/service/svc/version/3/backend/origin":
			_, _ = w.Write([]byte(`{"address":"198.51.100.1","port":80}`))
		case "/service/svc/version/3/clone":
			if r.Method != http.MethodPut {
				t.Fatalf("clone method = %s", r.Method)
			}
			clones.Add(1)
			_, _ = w.Write([]byte(`{"number":4,"active":false}`))
		case "/service/svc/version/4/backend/origin":
			if r.Method != http.MethodPut {
				t.Fatalf("backend method = %s", r.Method)
			}
			if err := r.ParseForm(); err != nil {
				t.Fatal(err)
			}
			if r.Form.Get("address") != "203.0.113.5" || r.Form.Get("port") != "26656" {
				t.Fatalf("bad form: %s", r.Form.Encode())
			}
			updates.Add(1)
			_, _ = w.Write([]byte(`{"address":"203.0.113.5","port":26656}`))
		case "/service/svc/version/4/validate":
			_, _ = w.Write([]byte(`{"status":"ok","msg":""}`))
		case "/service/svc/version/4/activate":
			if r.Method != http.MethodPut {
				t.Fatalf("activate method = %s", r.Method)
			}
			activates.Add(1)
			_, _ = w.Write([]byte(`{"number":4,"active":true}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer s.Close()

	c := New(Config{
		APIKey:      "token",
		ServiceID:   "svc",
		BackendName: "origin",
		APIBase:     s.URL,
	})
	addr := netip.MustParseAddrPort("203.0.113.5:26656")
	if err := c.Sync(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	if err := c.Sync(context.Background(), addr); err != nil {
		t.Fatal(err)
	}
	if clones.Load() != 1 || updates.Load() != 1 || activates.Load() != 1 {
		t.Fatalf("clones=%d updates=%d activates=%d", clones.Load(), updates.Load(), activates.Load())
	}
}

func TestUpdateSkipsUnchangedBackend(t *testing.T) {
	var clones atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/service/svc":
			_, _ = w.Write([]byte(`{"versions":[{"number":3,"active":true}]}`))
		case "/service/svc/version/3/backend/origin":
			_, _ = w.Write([]byte(`{"address":"203.0.113.6","port":` + strconv.Itoa(443) + `}`))
		case "/service/svc/version/3/clone":
			clones.Add(1)
			_, _ = w.Write([]byte(`{"number":4}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer s.Close()

	c := New(Config{
		APIKey:      "token",
		ServiceID:   "svc",
		BackendName: "origin",
		APIBase:     s.URL,
	})
	if err := c.Update(context.Background(), netip.MustParseAddrPort("203.0.113.6:443")); err != nil {
		t.Fatal(err)
	}
	if clones.Load() != 0 {
		t.Fatalf("clone called %d times", clones.Load())
	}
}

func TestReadResponseBodyTooLarge(t *testing.T) {
	_, err := readResponseBody(strings.NewReader(strings.Repeat("x", maxResponseBodySize+1)), -1, maxResponseBodySize, nil)
	if !errors.Is(err, errResponseBodyTooLarge) {
		t.Fatalf("got %v; want %v", err, errResponseBodyTooLarge)
	}
}
