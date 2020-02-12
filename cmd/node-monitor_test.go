package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"testing"
	"time"
)

const unreachableHost = "192.168.2.1:9000"

func nodeMonitorTestServer(addr string) (start func(), stop func()) {
	if addr == unreachableHost {
		return func() {}, func() {}
	}
	var srv *http.Server
	started := false

	start = func() {
		if started {
			return
		}
		started = true
		srv = &http.Server{Addr: addr}
		// log.Printf("%s started", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("%s ListenAndServe(): %v", addr, err)
		}
		// log.Printf("%s stopped", addr)
	}

	stop = func() {
		if !started {
			return
		}
		// log.Printf("%s stopping", addr)
		if err := srv.Shutdown(context.TODO()); err != nil {
			log.Fatalf("%s Shutdown(): %v", addr, err)
		}
		started = false
	}

	go start()
	time.Sleep(time.Second)

	return start, stop
}

func TestNodeMonitor(t *testing.T) {
	http.HandleFunc(path.Join(healthCheckPathPrefix, healthCheckReadinessPath), func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
	})

	for i, input := range []struct {
		unreachable bool // last host will be unreachable if set true
		isLocal     bool // first host will be localhost if set true
		num         int  // num of other test servers
		opts        [][2]int
		res         [][]int
	}{
		{
			false,
			false,
			1,
			[][2]int{{0, 0}, {0, 1}},
			[][]int{{0}, {1}},
		},
		{
			false,
			false,
			3,
			[][2]int{{0, 0}, {1, 0}, {0, 1}},
			[][]int{{0, 1, 1}, {0, 0, 1}, {1, 0, 1}},
		},
		{
			true,
			true,
			1,
			[][2]int{{1, 0}, {1, 1}},
			[][]int{{1, 0, 0}, {1, 1, 0}},
		},
	} {
		endpoints := make(EndpointList, 0, input.num+2)
		if input.isLocal {
			u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:9000"))
			if err != nil {
				t.Fatal(err)
			}
			endpoints = append(endpoints, Endpoint{
				URL:     u,
				IsLocal: true,
			})
		}
		for j := 0; j < input.num; j++ {
			u, err := url.Parse(fmt.Sprintf("http://127.0.0.1:%d", 9001+j))
			if err != nil {
				t.Fatal(err)
			}
			endpoints = append(endpoints, Endpoint{
				URL:     u,
				IsLocal: false,
			})
		}
		if input.unreachable {
			u, err := url.Parse(fmt.Sprintf("http://%s", unreachableHost))
			if err != nil {
				t.Fatal(err)
			}
			endpoints = append(endpoints, Endpoint{
				URL:     u,
				IsLocal: false,
			})
		}

		servers := make([][2]func(), 0, input.num+2)
		for _, e := range endpoints {
			start, stop := nodeMonitorTestServer(e.Host)
			servers = append(servers, [2]func(){start, stop})
		}

		nm, err := NewNodeMonitor(endpoints)
		if err != nil {
			t.Fatal(err)
		}

		onlines := make([]func() bool, 0, input.num+2)
		for _, e := range endpoints {
			onlines = append(onlines, nm.NodeOnlineFn(e.Host))
		}

		for s, opt := range input.opts {
			// log.Printf("option on %s: %d", endpoints[opt[0]].Host, opt[1])
			server := servers[opt[0]]
			switch opt[1] {
			case 0:
				// stop
				server[1]()
				time.Sleep(time.Second)
			case 1:
				// start
				go server[0]()
				time.Sleep(time.Second)
			default:
				t.Errorf("fuck")
			}
			// make sure the node is removed
			time.Sleep(defaultNodeMonitorRetryTimes * defaultNodeMonitorInterval * 2)

			for z := range endpoints {
				res := 0
				if onlines[z]() {
					res = 1
				}
				if res != input.res[s][z] {
					t.Errorf("[%d-%d]invalid status of %s, wanna %d, got %d", i, s, endpoints[z].Host, input.res[s][z], res)
				}
			}
			time.Sleep(time.Second)
		}
		for _, s := range servers {
			s[1]()
		}
		nm.Close()
	}
}
