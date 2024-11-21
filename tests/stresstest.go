package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var wg sync.WaitGroup
	numRequests := 1000
	concurrency := 50
	ports := []int{8080, 8081} // Adjust ports as needed
	key := "name"

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Channel to limit concurrency
	requestCh := make(chan struct{}, concurrency)

	var portIndex uint64 = 0
	portsLen := uint64(len(ports))

	start := time.Now()

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		requestCh <- struct{}{}
		go func() {
			defer wg.Done()
			// Choose one method:
			// Random Port Selection
			// port := ports[rand.Intn(len(ports))]

			// Round-Robin Port Selection
			idx := atomic.AddUint64(&portIndex, 1)
			port := ports[idx%portsLen]

			url := fmt.Sprintf("http://localhost:%d/get?key=%s", port, key)
			resp, err := http.Get(url)
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
	fmt.Printf("Total time for %d requests: %s\n", numRequests, elapsed)
	fmt.Printf("Average time per request: %s\n", elapsed/time.Duration(numRequests))
}
