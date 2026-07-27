package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Result struct {
	Name    string
	URL     string
	Status  int
	Elapsed time.Duration
	Err     error
}

// TargetState remembers what we last knew about a target, so we can
// detect when something CHANGES (up -> down, or down -> up).
type TargetState struct {
	IsUp                bool      // true if the last check succeeded
	ConsecutiveFailures int       // how many checks in a row have failed
	LastChanged         time.Time // when did IsUp last flip?
}

// Monitor bundles together everything the whole program needs to share
// safely across goroutines: the state map itself, AND a Mutex to protect it.
type Monitor struct {
	mu     sync.Mutex              // the "lock" - protects the map below
	states map[string]*TargetState // name -> current known state
}

// NewMonitor creates a Monitor with an empty, initialized states map.
func NewMonitor() *Monitor {
	return &Monitor{
		states: make(map[string]*TargetState),
	}
}

// update takes a fresh Result and updates our stored state for that target.
// It returns true if the target's up/down status CHANGED since last time,
// so the caller knows whether to print an alert.
func (m *Monitor) update(r Result) (changed bool, newState TargetState) {
	// Lock before touching the shared map - only one goroutine can be
	// inside this function's critical section at a time.
	m.mu.Lock()
	defer m.mu.Unlock() // guarantees we unlock even if we return early

	isUp := r.Err == nil // no error means the check succeeded

	// Look up existing state for this target, if we have one yet.
	state, exists := m.states[r.Name]

	if !exists {
		// First time seeing this target - create a fresh state entry.
		state = &TargetState{
			IsUp:        isUp,
			LastChanged: time.Now(),
		}
		m.states[r.Name] = state

		if !isUp {
			state.ConsecutiveFailures = 1
		}

		// We treat the very first check as "changed" so it gets printed once.
		return true, *state
	}

	// Did the up/down status flip since last time?
	statusChanged := state.IsUp != isUp

	if statusChanged {
		state.IsUp = isUp
		state.LastChanged = time.Now()
	}

	if isUp {
		state.ConsecutiveFailures = 0
	} else {
		state.ConsecutiveFailures = state.ConsecutiveFailures + 1
	}

	// Return a COPY of the state (*state dereferences the pointer to copy
	// the struct's current values) so the caller can print details safely
	// without needing to hold the lock themselves.
	return statusChanged, *state
}

func loadTargets(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var targets map[string]string
	err = json.Unmarshal(data, &targets)
	if err != nil {
		return nil, err
	}
	return targets, nil
}

func checkURL(ctx context.Context, name, url string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		results <- Result{Name: name, URL: url, Err: err}
		return
	}

	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		results <- Result{Name: name, URL: url, Err: err}
		return
	}
	defer resp.Body.Close()

	results <- Result{Name: name, URL: url, Status: resp.StatusCode, Elapsed: elapsed}
}

// runChecks now takes a *Monitor so it can update shared state and decide
// whether to print an alert for each result.
func runChecks(targets map[string]string, monitor *Monitor) {
	results := make(chan Result)
	var wg sync.WaitGroup

	for name, url := range targets {
		wg.Add(1)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		go checkURL(ctx, name, url, results, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		// Feed this result into the monitor; find out if status changed.
		changed, state := monitor.update(r)

		if !changed {
			// Nothing changed - skip printing to avoid spam. (You could
			// still log this somewhere later, but for now we stay quiet.)
			continue
		}

		// Something changed - print an alert-style message.
		if state.IsUp {
			fmt.Printf("[ALERT] %s (%s) is now UP (took %v)\n", r.Name, r.URL, r.Elapsed)
		} else {
			fmt.Printf("[ALERT] %s (%s) is now DOWN: %v\n", r.Name, r.URL, r.Err)
		}
	}
}

func main() {
	targets, err := loadTargets("targets.json")
	if err != nil {
		fmt.Println("failed to load targets:", err)
		return
	}

	// Create one shared Monitor for the whole program's lifetime.
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
