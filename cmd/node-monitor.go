package cmd

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/pkg/set"
	"github.com/minio/minio/cmd/logger"
	xnet "github.com/minio/minio/pkg/net"
	"golang.org/x/time/rate"
)

var globalNodeMonitor *NodeMonitor

const defaultNodeMonitorInterval = 3 * time.Second
const defaultNodeMonitorRetryTimes = 3

type NodeMonitor struct {
	clients map[string]*nodeMonitorClient
	done    chan struct{}
	wg      sync.WaitGroup
}

func NewNodeMonitor(endpoints EndpointList) (*NodeMonitor, error) {
	m := NodeMonitor{
		clients: make(map[string]*nodeMonitorClient),
		done:    make(chan struct{}),
	}
	seenHosts := set.NewStringSet()
	for _, endpoint := range endpoints {
		if seenHosts.Contains(endpoint.Host) {
			continue
		}
		seenHosts.Add(endpoint.Host)

		if endpoint.IsLocal {
			// always online client
			m.clients[endpoint.Host] = &nodeMonitorClient{failed: 0, retry: 3}
		} else {
			c, err := newNodeMonitorClient(endpoint, defaultNodeMonitorRetryTimes)
			if err != nil {
				m.Close()
				return nil, err
			}
			m.wg.Add(1)
			go c.probeEvery(m.done, &(m.wg), defaultNodeMonitorInterval)
			m.clients[endpoint.Host] = c
		}
	}
	return &m, nil
}

func (m *NodeMonitor) NodeOnlineFn(host string) func() bool {
	return func() bool {
		c, ok := m.clients[host]
		if !ok {
			return false
		}
		return c.isOnline()
	}
}

func (m *NodeMonitor) AllOnline() bool {
	for _, c := range m.clients {
		if c == nil {
			return false
		}
		if !c.isOnline() {
			return false
		}
	}
	return true
}

func (m *NodeMonitor) Close() {
	close(m.done)
	m.wg.Wait()
	for _, c := range m.clients {
		if c == nil {
			continue
		}
		c.close()
	}
}

type nodeMonitorClient struct {
	host   string
	url    string
	client *http.Client
	failed uint64
	retry  uint64
}

func (c *nodeMonitorClient) isOnline() bool {
	return atomic.LoadUint64(&c.failed) < c.retry
}

func (c *nodeMonitorClient) probeEvery(done chan struct{}, wg *sync.WaitGroup, interval time.Duration) {
	defer wg.Done()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	limiter := rate.NewLimiter(rate.Every(interval/time.Duration(c.retry)), 1)
	ch := make(chan bool, c.retry)
	for i := uint64(0); i < c.retry; i++ {
		go c.probe(ctx, limiter, ch)
	}

	var alive bool
	for {
		timer := time.NewTimer(interval)
		select {
		case alive = <-ch:
			timer.Stop()
		case <-timer.C:
			//timeout
			alive = false
		case <-done:
			return
		}
		c.handle(alive)
	}
}

func (c *nodeMonitorClient) handle(alive bool) {
	if alive {
		if !c.isOnline() {
			logger.Info("%s is marked as online", c.host)
		}
		atomic.StoreUint64(&c.failed, 0)
	} else {
		times := atomic.AddUint64(&(c.failed), 1)
		if times == c.retry {
			logger.Info("%s is marked as offline", c.host)
		}
	}
}

func (c *nodeMonitorClient) probe(ctx context.Context, limiter *rate.Limiter, alive chan bool) {
	for {
		if err := limiter.Wait(ctx); err != nil {
			return
		}
		resp, err := c.client.Head(c.url)
		if err != nil {
			// logger.Info("failed to probe host %s: %s", c.url, err.Error())
			continue
		}
		defer resp.Body.Close()
		alive <- resp.StatusCode == http.StatusOK
	}
}

func (c *nodeMonitorClient) close() {
	atomic.StoreUint64(&(c.failed), c.retry)
}

func newNodeMonitorClient(endpoint Endpoint, retry uint64) (*nodeMonitorClient, error) {
	host, err := xnet.ParseHost(endpoint.Host)
	if err != nil {
		return nil, err
	}

	scheme := "http"
	if globalIsSSL {
		scheme = "https"
	}

	serverURL := &url.URL{
		Scheme: scheme,
		Host:   endpoint.Host,
		Path:   path.Join(healthCheckPathPrefix, healthCheckReadinessPath),
	}

	var tlsConfig *tls.Config
	if globalIsSSL {
		tlsConfig = &tls.Config{
			ServerName: host.Name,
			RootCAs:    globalRootCAs,
		}
	}

	trFn := newCustomHTTPTransport(tlsConfig, time.Second, time.Second)

	return &nodeMonitorClient{
		client: &http.Client{Transport: trFn()},
		host:   serverURL.Host,
		url:    serverURL.String(),
		failed: 0,
		retry:  retry,
	}, nil
}
