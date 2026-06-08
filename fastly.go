package natfy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/csbxd/natnet"
)

const defaultAPIBase = "https://api.fastly.com"
const maxResponseBodySize = 64 * 1024

var (
	errResponseBodyTooLarge = errors.New("response body too large")
	responseBodyPool        = sync.Pool{
		New: func() any {
			b := make([]byte, 0, 1024)
			return &b
		},
	}
)

type Config struct {
	APIKey      string
	ServiceID   string
	BackendName string

	Retry      int
	Timeout    time.Duration
	APIBase    string
	HTTPClient *http.Client
}

type Client struct {
	cfg Config
	hc  *http.Client

	mu     sync.Mutex
	synced bool
	last   natnet.Addr
}

func New(cfg Config) *Client {
	if cfg.Retry == 0 {
		cfg.Retry = 3
	}
	if cfg.APIBase == "" {
		cfg.APIBase = defaultAPIBase
	}
	cfg.APIBase = strings.TrimRight(cfg.APIBase, "/")
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: cfg.Timeout}
	}
	return &Client{
		cfg: cfg,
		hc:  hc,
	}
}

func (c *Client) Sync(ctx context.Context, addr natnet.Addr) error {
	c.mu.Lock()
	if c.synced && c.last == addr {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.Update(ctx, addr); err != nil {
		return err
	}

	c.mu.Lock()
	c.synced = true
	c.last = addr
	c.mu.Unlock()
	return nil
}

func (c *Client) Update(ctx context.Context, addr natnet.Addr) error {
	active, err := c.activeVersion(ctx)
	if err != nil {
		return err
	}
	backend, err := c.backend(ctx, active)
	if err != nil {
		return err
	}
	if backend.Address == addr.Addr().String() && backend.Port == int(addr.Port()) {
		return nil
	}

	cloned, err := c.cloneVersion(ctx, active)
	if err != nil {
		return err
	}
	if err := c.updateBackend(ctx, cloned, addr); err != nil {
		return err
	}
	if err := c.validateVersion(ctx, cloned); err != nil {
		return err
	}
	return c.activateVersion(ctx, cloned)
}

func (c *Client) activeVersion(ctx context.Context) (int, error) {
	var s service
	if err := c.doJSON(ctx, http.MethodGet, path("service", c.cfg.ServiceID), nil, &s); err != nil {
		return 0, err
	}
	for i := range s.Versions {
		if s.Versions[i].Active {
			return s.Versions[i].Number, nil
		}
	}
	return 0, errors.New("fastly active version not found")
}

func (c *Client) backend(ctx context.Context, version int) (backend, error) {
	var b backend
	err := c.doJSON(ctx, http.MethodGet, path("service", c.cfg.ServiceID, "version", strconv.Itoa(version), "backend", c.cfg.BackendName), nil, &b)
	return b, err
}

func (c *Client) cloneVersion(ctx context.Context, version int) (int, error) {
	var v serviceVersion
	if err := c.doJSON(ctx, http.MethodPut, path("service", c.cfg.ServiceID, "version", strconv.Itoa(version), "clone"), nil, &v); err != nil {
		return 0, err
	}
	if v.Number == 0 {
		return 0, errors.New("fastly clone response has no version number")
	}
	return v.Number, nil
}

func (c *Client) updateBackend(ctx context.Context, version int, addr natnet.Addr) error {
	values := url.Values{
		"address": []string{addr.Addr().String()},
		"port":    []string{strconv.Itoa(int(addr.Port()))},
	}
	var b backend
	return c.doForm(ctx, http.MethodPut, path("service", c.cfg.ServiceID, "version", strconv.Itoa(version), "backend", c.cfg.BackendName), values, &b)
}

func (c *Client) validateVersion(ctx context.Context, version int) error {
	var s status
	if err := c.doJSON(ctx, http.MethodGet, path("service", c.cfg.ServiceID, "version", strconv.Itoa(version), "validate"), nil, &s); err != nil {
		return err
	}
	if s.Status != "ok" {
		if s.Msg == "" {
			return errors.New("fastly version validation failed")
		}
		return errors.New(s.Msg)
	}
	return nil
}

func (c *Client) activateVersion(ctx context.Context, version int) error {
	var v serviceVersion
	return c.doJSON(ctx, http.MethodPut, path("service", c.cfg.ServiceID, "version", strconv.Itoa(version), "activate"), nil, &v)
}

func (c *Client) doJSON(ctx context.Context, method, path string, in any, out any) error {
	var body []byte
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = b
	}
	return c.do(ctx, method, path, body, "application/json", out)
}

func (c *Client) doForm(ctx context.Context, method, path string, values url.Values, out any) error {
	return c.do(ctx, method, path, []byte(values.Encode()), "application/x-www-form-urlencoded", out)
}

func (c *Client) do(ctx context.Context, method, path string, body []byte, contentType string, out any) error {
	var err error
	for i := 0; i <= c.cfg.Retry; i++ {
		err = c.doOnce(ctx, method, path, body, contentType, out)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return err
}

func (c *Client) doOnce(ctx context.Context, method, path string, body []byte, contentType string, out any) error {
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.cfg.APIBase+"/"+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Fastly-Key", c.cfg.APIKey)
	if contentType != "" && body != nil {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBodyBuf := acquireResponseBody()
	defer releaseResponseBody(respBodyBuf)
	respBody, err := readResponseBody(resp.Body, resp.ContentLength, maxResponseBodySize, *respBodyBuf)
	*respBodyBuf = respBody
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("fastly %s %s: %s: %s", method, path, resp.Status, trimBody(respBody))
	}
	if out == nil || len(respBody) == 0 {
		return nil
	}
	return json.Unmarshal(respBody, out)
}

func path(parts ...string) string {
	var b strings.Builder
	for i := range parts {
		if i > 0 {
			b.WriteByte('/')
		}
		b.WriteString(url.PathEscape(parts[i]))
	}
	return b.String()
}

func trimBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 512 {
		return s[:512]
	}
	return s
}

func acquireResponseBody() *[]byte {
	b := responseBodyPool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

func releaseResponseBody(b *[]byte) {
	if cap(*b) <= maxResponseBodySize {
		responseBodyPool.Put(b)
	}
}

func readResponseBody(r io.Reader, contentLength int64, maxBodySize int, dst []byte) ([]byte, error) {
	if maxBodySize > 0 && contentLength > int64(maxBodySize) {
		return dst, errResponseBodyTooLarge
	}
	dst = dst[:0]
	if contentLength == 0 {
		return dst, nil
	}
	if contentLength > 0 {
		n := int(contentLength)
		if cap(dst) < n {
			dst = make([]byte, n)
		} else {
			dst = dst[:n]
		}
		_, err := io.ReadFull(r, dst)
		return dst, err
	}
	if cap(dst) == 0 {
		dst = make([]byte, 0, 1024)
	}
	for {
		if maxBodySize > 0 && len(dst) == maxBodySize {
			var b [1]byte
			n, err := r.Read(b[:])
			if n > 0 {
				return dst, errResponseBodyTooLarge
			}
			if errors.Is(err, io.EOF) {
				return dst, nil
			}
			if err != nil {
				return dst, err
			}
			return dst, errors.New("response body read returned (0, nil)")
		}
		if len(dst) == cap(dst) {
			n := 2 * cap(dst)
			if n == 0 {
				n = 1024
			}
			if maxBodySize > 0 && n > maxBodySize {
				n = maxBodySize
			}
			b := make([]byte, len(dst), n)
			copy(b, dst)
			dst = b
		}
		n, err := r.Read(dst[len(dst):cap(dst)])
		dst = dst[:len(dst)+n]
		if errors.Is(err, io.EOF) {
			return dst, nil
		}
		if err != nil {
			return dst, err
		}
		if n == 0 {
			return dst, errors.New("response body read returned (0, nil)")
		}
	}
}

type service struct {
	Versions []serviceVersion `json:"versions"`
}

type serviceVersion struct {
	Number int  `json:"number"`
	Active bool `json:"active"`
}

type backend struct {
	Address string `json:"address"`
	Port    int    `json:"port"`
}

type status struct {
	Status string `json:"status"`
	Msg    string `json:"msg"`
}
