package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type SproxydClient struct {
	endpoint string
	path     string
	client   *http.Client
}

type SproxydObjectInfo struct {
	ContentLength int64
	UserMD        string // raw x-scal-usermd header value (base64-encoded JSON)
}

func NewSproxydClient(endpoint, path string) *SproxydClient {
	return &SproxydClient{
		endpoint: endpoint,
		path:     path,
		client: &http.Client{
			Timeout: 5 * time.Minute,
		},
	}
}

func (s *SproxydClient) keyURL(key string) string {
	return fmt.Sprintf("%s%s/%s", s.endpoint, s.path, key)
}

// Head returns object metadata without fetching the body.
// Returns nil info and nil error if the key does not exist (404).
func (s *SproxydClient) Head(key string) (*SproxydObjectInfo, error) {
	url := s.keyURL(key)
	resp, err := s.client.Head(url)
	if err != nil {
		return nil, fmt.Errorf("HEAD %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HEAD %s returned %d", url, resp.StatusCode)
	}

	cl, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil || cl <= 0 {
		return nil, fmt.Errorf("HEAD %s: missing or invalid Content-Length header", url)
	}
	return &SproxydObjectInfo{
		ContentLength: cl,
		UserMD:        resp.Header.Get("X-Scal-Usermd"),
	}, nil
}

// Get fetches the object body. Caller must close the returned ReadCloser.
// Returns nil body and nil error if the key does not exist (404).
func (s *SproxydClient) Get(key string) (io.ReadCloser, *SproxydObjectInfo, error) {
	url := s.keyURL(key)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, nil, fmt.Errorf("GET %s: %w", url, err)
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("GET %s returned %d", url, resp.StatusCode)
	}

	cl, err := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	if err != nil || cl <= 0 {
		resp.Body.Close()
		return nil, nil, fmt.Errorf("GET %s: missing or invalid Content-Length header", url)
	}
	info := &SproxydObjectInfo{
		ContentLength: cl,
		UserMD:        resp.Header.Get("X-Scal-Usermd"),
	}
	return resp.Body, info, nil
}

// Put writes a body to the given key, preserving the original metadata.
func (s *SproxydClient) Put(key string, body io.Reader, info *SproxydObjectInfo) error {
	url := s.keyURL(key)
	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return fmt.Errorf("creating PUT request: %w", err)
	}

	req.ContentLength = info.ContentLength
	if info.UserMD != "" {
		req.Header.Set("X-Scal-Usermd", info.UserMD)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("PUT %s: %w", url, err)
	}
	respBody, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return fmt.Errorf("PUT %s: reading response: %w", url, err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("PUT %s returned %d: %s", url, resp.StatusCode, string(respBody))
	}
	return nil
}
