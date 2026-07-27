package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
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

func checkURL(name, url string, results chan<- Result) {
	start := time.Now()
	resp, err := http.Get(url)
	elapsed := time.Since(start)

	if err != nil {
		results <- Result{Name: name, URL: url, Err: err}
		return
	}
	defer resp.Body.Close()

	results <- Result{Name: name, URL: url, Status: resp.StatusCode, Elapsed: elapsed}
}

func main() {
	targets, err := loadTargets("targets.json")
	if err != nil {
		fmt.Println("failed to load targets:", err)
		return
	}

	results := make(chan Result)

	for name, url := range targets {
		go checkURL(name, url, results)
	}

	for i := 0; i < len(targets); i++ {
		r := <-results
		if r.Err != nil {
			fmt.Printf("[%s] %s -> ERROR: %v\n", r.Name, r.URL, r.Err)
		} else {
			fmt.Printf("[%s] %s -> status %d, took %v\n", r.Name, r.URL, r.Status, r.Elapsed)
		}
	}
}
