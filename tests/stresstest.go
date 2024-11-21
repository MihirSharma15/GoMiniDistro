package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func benchmarkRequests(numRequests int, numPorts int) {
	var wg sync.WaitGroup
	concurrency := 25
	ports := []int{8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088}
	key := "name"

	// Check if numPorts is valid
	if numPorts < 1 || numPorts > len(ports) {
		fmt.Printf("Invalid number of ports: %d\n", numPorts)
		return
	}

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Channel to limit concurrency
	requestCh := make(chan struct{}, concurrency)

	var portIndex uint64 = 0
	portsLen := uint64(numPorts)

	// Create a single http.Client
	client := &http.Client{}

	// Select the first numPorts from ports
	selectedPorts := ports[:numPorts]

	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		requestCh <- struct{}{}
		go func() {
			defer wg.Done()
			// Round-Robin Port Selection
			idx := atomic.AddUint64(&portIndex, 1)
			port := selectedPorts[idx%portsLen]

			url := fmt.Sprintf("http://localhost:%d/get?key=%s", port, key)
			resp, err := client.Get(url) // Use the single client
			if err != nil {
				fmt.Println("Error:", err)
			} else {
				resp.Body.Close()
			}
			<-requestCh
		}()
	}

	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("Total time for %d requests using %d ports: %s\n", numRequests, numPorts, elapsed)
	fmt.Printf("Average time per request: %s\n\n", elapsed/time.Duration(numRequests))
}

func main() {
	numRequests := 1000
	for numPorts := 1; numPorts <= len([]int{8080, 8081, 8082, 8083, 8084, 8085, 8086, 8087, 8088}); numPorts++ {
		benchmarkRequests(numRequests, numPorts)
	}
}
