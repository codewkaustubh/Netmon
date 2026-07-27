package main

// "encoding/json" lets us parse JSON text into Go data structures (like our targets.json)
// "fmt" is for printing output to the terminal
// "net/http" lets us make HTTP requests (the actual "check if it's up" logic)
// "os" lets us read files and command-line stuff
// "sync" gives us WaitGroup and Mutex - tools for safely coordinating goroutines
// "time" lets us measure how long requests take
import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"
)

// Result is a custom struct (a bundle of related fields) that each goroutine
// will fill in and send back, describing what happened when it checked one target.
type Result struct {
	Name    string        // friendly name, e.g. "google"
	URL     string        // the actual URL that was checked
	Status  int           // HTTP status code, e.g. 200, 404, 500
	Elapsed time.Duration // how long the request took
	Err     error         // non-nil if something went wrong (DNS failure, timeout, etc.)
}

// loadTargets reads targets.json from disk and turns it into a Go map.
// It returns two things: the map of targets, AND an error.
// This "value, error" return pattern is everywhere in Go - callers are
// expected to check the error before trusting the value.
func loadTargets(path string) (map[string]string, error) {
	// os.ReadFile reads the entire file into memory as raw bytes ([]byte).
	data, err := os.ReadFile(path)
	if err != nil {
		// If we couldn't even read the file (e.g. it doesn't exist),
		// we return nil (no map) and the error explaining why.
		return nil, err
	}

	// Declare an empty map that will hold our name -> URL pairs.
	// "var targets map[string]string" creates a nil map variable;
	// json.Unmarshal will allocate and fill it for us.
	var targets map[string]string

	// json.Unmarshal parses the raw JSON bytes ("data") into our map.
	// We pass "&targets" (a POINTER to targets) because Unmarshal needs
	// to modify the actual variable, not just work with a copy of it.
	err = json.Unmarshal(data, &targets)
	if err != nil {
		return nil, err
	}

	// Success: hand back the parsed map, and nil for "no error".
	return targets, nil
}

// checkURL performs one HTTP check against a single target, then sends
// the outcome into the shared "results" channel.
//
// Parameters:
//
//	name    - friendly name of this target (for labeling output)
//	url     - the URL to check
//	results - a channel we SEND results into (chan<- means send-only here)
//	wg      - a pointer to the shared WaitGroup, so we can call wg.Done()
//	          when this goroutine finishes its work
func checkURL(name, url string, results chan<- Result, wg *sync.WaitGroup) {
	// This is CRITICAL: no matter how this function exits (normally or early
	// return), "defer wg.Done()" guarantees we decrement the WaitGroup counter
	// exactly once. Forgetting this would cause wg.Wait() in main() to hang forever.
	defer wg.Done()

	// Record the time right before we start the request.
	start := time.Now()

	// Make the actual HTTP GET request. This blocks (waits) until we get
	// a response or an error - but since this whole function runs inside
	// its own goroutine, it doesn't block anything else in the program.
	resp, err := http.Get(url)

	// Calculate how long the request took, now that it's done.
	elapsed := time.Since(start)

	// Check if something went wrong (DNS failure, connection refused, timeout, etc.)
	if err != nil {
		// Send a Result describing the failure into the channel.
		// The "<-" arrow means "send this value into the channel".
		results <- Result{Name: name, URL: url, Err: err}
		return // stop here; defer wg.Done() still runs on the way out
	}

	// defer here means "close the response body right before this function
	// returns". This is standard practice to avoid leaking network resources.
	defer resp.Body.Close()

	// Send a successful Result into the channel.
	results <- Result{Name: name, URL: url, Status: resp.StatusCode, Elapsed: elapsed}
}

func main() {
	// Load our targets (name -> URL pairs) from the JSON config file.
	targets, err := loadTargets("targets.json")
	if err != nil {
		fmt.Println("failed to load targets:", err)
		return // can't do anything without targets, so exit early
	}

	// Create a channel that will carry Result values from goroutines back to main.
	// This is an UNBUFFERED channel: a send will block until something receives it.
	results := make(chan Result)

	// Create a WaitGroup. This is our mechanism for knowing "have all the
	// goroutines I launched finished yet?"
	var wg sync.WaitGroup

	// Launch one goroutine per target.
	for name, url := range targets {
		// wg.Add(1) tells the WaitGroup "one more goroutine is starting -
		// don't let Wait() return until this one calls Done()".
		// IMPORTANT: Add must happen BEFORE the goroutine starts, in the
		// main goroutine, to avoid race conditions.
		wg.Add(1)

		// "go" launches checkURL as a new goroutine - it runs concurrently,
		// not blocking this loop from moving to the next target immediately.
		go checkURL(name, url, results, &wg)
	}

	// We need a separate goroutine to close the channel once all the
	// checkURL goroutines are done. Why? Because closing the channel is
	// how we'll tell the "receive results" loop below "no more values are
	// coming, stop waiting". We can't call wg.Wait() directly in main()
	// before receiving results, because wg.Wait() would block forever -
	// the checkURL goroutines can't finish sending on an unbuffered
	// channel until something is receiving from it.
	go func() {
		wg.Wait()      // block here until every goroutine has called wg.Done()
		close(results) // closing the channel signals "no more values coming"
	}()

	// Receive results as they arrive. Using "range" on a channel means:
	// "keep receiving values until the channel is closed".
	// This is cleaner than manually counting how many results to expect.
	for r := range results {
		if r.Err != nil {
			fmt.Printf("[%s] %s -> ERROR: %v\n", r.Name, r.URL, r.Err)
		} else {
			fmt.Printf("[%s] %s -> status %d, took %v\n", r.Name, r.URL, r.Status, r.Elapsed)
		}
	}
}
