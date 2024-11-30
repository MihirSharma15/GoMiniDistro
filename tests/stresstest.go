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
	concurrency := 5     // Reduced concurrency
	ports := []int{8080} // Use a single port
	key := "name"

	rand.Seed(time.Now().UnixNano())
	requestCh := make(chan struct{}, concurrency)

	var portIndex uint64 = 0
	portsLen := uint64(numPorts)

	// Custom Transport to increase idle connections
	transport := &http.Transport{
		MaxIdleConnsPerHost: 100,
	}
	client := &http.Client{
		Transport: transport,
	}

	selectedPorts := ports[:numPorts]
	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		requestCh <- struct{}{}
		go func() {
			defer wg.Done()
			idx := atomic.AddUint64(&portIndex, 1)
			port := selectedPorts[idx%portsLen]

			url := fmt.Sprintf("http://localhost:%d/get?key=%s", port, key)
			resp, err := client.Get(url)
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
	numRequests := 10000
	numPorts := 1 // Using only one port
	benchmarkRequests(numRequests, numPorts)
}
