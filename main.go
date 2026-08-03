package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Result struct {
	Name    string
	Target  string
	Status  int // only meaningful for HTTP checks; 0 for TCP
	Elapsed time.Duration
	Err     error
}

type TargetState struct {
	IsUp                bool
	ConsecutiveFailures int
	LastChanged         time.Time
}

type Monitor struct {
	mu     sync.Mutex
	states map[string]*TargetState
}

func NewMonitor() *Monitor {
	return &Monitor{states: make(map[string]*TargetState)}
}

func (m *Monitor) update(r Result) (changed bool, newState TargetState) {
	m.mu.Lock()
	defer m.mu.Unlock()

	isUp := r.Err == nil
	state, exists := m.states[r.Name]

	if !exists {
		state = &TargetState{IsUp: isUp, LastChanged: time.Now()}
		m.states[r.Name] = state
		if !isUp {
			state.ConsecutiveFailures = 1
		}
		return true, *state
	}

	statusChanged := state.IsUp != isUp
	if statusChanged {
		state.IsUp = isUp
		state.LastChanged = time.Now()
	}
	if isUp {
		state.ConsecutiveFailures = 0
	} else {
		state.ConsecutiveFailures++
	}

	return statusChanged, *state
}

// Target now describes HOW to check something (its "Type": "http" or "tcp")
// and WHAT to check (its "Target": a URL or a host:port string).
type Target struct {
	Type   string `json:"type"`   // "http" or "tcp" - the json tags tell
	Target string `json:"target"` // encoding/json which JSON field maps to which Go field
}

// loadTargets now returns a map of name -> Target (instead of name -> string),
// since each target now carries both a type and a value.
func loadTargets(path string) (map[string]Target, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var targets map[string]Target
	err = json.Unmarshal(data, &targets)
	if err != nil {
		return nil, err
	}
	return targets, nil
}

// checkHTTP performs an HTTP GET check, same as before.
func checkHTTP(ctx context.Context, name, url string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		results <- Result{Name: name, Target: url, Err: err}
		return
	}

	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		results <- Result{Name: name, Target: url, Err: err}
		return
	}
	defer resp.Body.Close()

	results <- Result{Name: name, Target: url, Status: resp.StatusCode, Elapsed: elapsed}
}

// checkTCP attempts a raw TCP connection to "host:port". If it connects
// successfully, the target is considered "up" - we don't care about any
// data, just whether the connection opened at all.
func checkTCP(ctx context.Context, name, hostport string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	// net.Dialer lets us dial WITH a context, so our timeout still applies
	// here just like it does for HTTP checks.
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", hostport)
	elapsed := time.Since(start)

	if err != nil {
		results <- Result{Name: name, Target: hostport, Err: err}
		return
	}
	// We successfully connected - close the connection immediately,
	// we only needed to prove it opens, not use it.
	conn.Close()

	results <- Result{Name: name, Target: hostport, Elapsed: elapsed}
}

// runChecks now looks at each target's Type to decide which checker to use.
func runChecks(targets map[string]Target, monitor *Monitor) {
	results := make(chan Result)
	var wg sync.WaitGroup

	for name, t := range targets {
		wg.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Dispatch to the right checker function based on the "type" field.
		switch t.Type {
		case "http":
			go checkHTTP(ctx, name, t.Target, results, &wg)
		case "tcp":
			go checkTCP(ctx, name, t.Target, results, &wg)
		default:
			// Unknown type - treat as an immediate failure, but we still
			// need to call wg.Done() and send a result, or things hang.
			wg.Done()
			results <- Result{Name: name, Target: t.Target, Err: fmt.Errorf("unknown target type: %s", t.Type)}
		}
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		changed, state := monitor.update(r)
		if !changed {
			continue
		}
		if state.IsUp {
			fmt.Printf("[ALERT] %s (%s) is now UP (took %v)\n", r.Name, r.Target, r.Elapsed)
		} else {
			fmt.Printf("[ALERT] %s (%s) is now DOWN: %v\n", r.Name, r.Target, r.Err)
		}
	}
}

func main() {
	targets, err := loadTargets("targets.json")
	if err != nil {
		fmt.Println("failed to load targets:", err)
		return
	}

	monitor := NewMonitor()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	fmt.Println("netmon started. Checking every 5 seconds. Press Ctrl+C to stop.")

	runChecks(targets, monitor)

	for {
		select {
		case <-ticker.C:
			runChecks(targets, monitor)
		case <-stop:
			fmt.Println("\nshutting down netmon...")
			return
		}
	}
}
