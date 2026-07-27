package main

import (
	"context"       // lets us set timeouts/cancellation on requests
	"encoding/json" // parse targets.json
	"fmt"
	"net/http"
	"os"
	"os/signal" // lets us listen for Ctrl+C
	"sync"
	"syscall" // defines the specific signal types (like SIGINT for Ctrl+C)
	"time"
)

type Result struct {
	Name    string
	URL     string
	Status  int
	Elapsed time.Duration
	Err     error
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

// checkURL now takes a "ctx context.Context" parameter - this carries the
// timeout information. Instead of using http.Get (which has no timeout
// built in), we build the request manually so we can attach the context.
func checkURL(ctx context.Context, name, url string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	start := time.Now()

	// Build an HTTP request "tied to" our context (ctx). If ctx's timeout
	// expires before the server responds, this request will be cancelled
	// automatically instead of hanging forever.
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		results <- Result{Name: name, URL: url, Err: err}
		return
	}

	// http.DefaultClient.Do actually sends the request (this is what
	// http.Get was doing internally, but now it respects our context/timeout).
	resp, err := http.DefaultClient.Do(req)
	elapsed := time.Since(start)

	if err != nil {
		// If the timeout expired, this error will mention "context deadline exceeded"
		results <- Result{Name: name, URL: url, Err: err}
		return
	}
	defer resp.Body.Close()

	results <- Result{Name: name, URL: url, Status: resp.StatusCode, Elapsed: elapsed}
}

// runChecks performs ONE full round of checking all targets concurrently,
// and prints the results. This is basically our old main() logic, now
// pulled out into its own function so the Ticker loop can call it repeatedly.
func runChecks(targets map[string]string) {
	results := make(chan Result)
	var wg sync.WaitGroup

	for name, url := range targets {
		wg.Add(1)

		// context.WithTimeout creates a context that automatically "expires"
		// after the given duration (5 seconds here). "cancel" is a function
		// we MUST call to release resources associated with the context,
		// even if it already expired naturally - hence the defer below.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel() // clean up this context once runChecks returns

		go checkURL(ctx, name, url, results, &wg)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		if r.Err != nil {
			fmt.Printf("[%s] %s -> ERROR: %v\n", r.Name, r.URL, r.Err)
		} else {
			fmt.Printf("[%s] %s -> status %d, took %v\n", r.Name, r.URL, r.Status, r.Elapsed)
		}
	}
}

func main() {
	targets, err := loadTargets("targets.json")
	if err != nil {
		fmt.Println("failed to load targets:", err)
		return
	}

	// Create a ticker that "fires" every 5 seconds. ticker.C is a channel
	// that receives a value each time the interval elapses.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop() // always stop a ticker when done, to free resources

	// Create a channel to receive OS interrupt signals (like Ctrl+C).
	stop := make(chan os.Signal, 1)
	// signal.Notify tells Go "send SIGINT (Ctrl+C) or SIGTERM notifications
	// into the 'stop' channel instead of just killing the program immediately."
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	fmt.Println("netmon started. Checking every 5 seconds. Press Ctrl+C to stop.")

	// Run one round of checks immediately, so we don't wait 5 seconds
	// before seeing any output at all.
	runChecks(targets)

	// This is the main event loop. "select" waits for ONE of multiple
	// channels to have something ready, and runs the matching case.
	for {
		select {
		case <-ticker.C:
			// The ticker fired - time for another round of checks.
			fmt.Println("\n--- running checks ---")
			runChecks(targets)

		case <-stop:
			// We received Ctrl+C (or a termination signal) - exit cleanly.
			fmt.Println("\nshutting down netmon...")
			return
		}
	}
}
