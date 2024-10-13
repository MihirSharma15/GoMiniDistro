package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type Node struct {
	data map[string]string
	mu   sync.RWMutex
}

func newNode() *Node {
	return &Node{
		data: make(map[string]string),
	}
}

func (n *Node) Put(w http.ResponseWriter, r *http.Request) {

	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	key, keyOk := body["key"]
	value, valueOk := body["value"]
	if !keyOk || !valueOk {
		http.Error(w, "Missing key or value in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.data[key] = value
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Stored: %s -> %s\n", key, value)
}

func (n *Node) Get(w http.ResponseWriter, r *http.Request) {
	// Extract key from the query parameters
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	// Retrieve the value from the database
	n.mu.RLock()
	value, exists := n.data[key]
	n.mu.RUnlock()

	// Respond to the client
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Value: %s\n", value)
}

// Delete removes a key-value pair from the node
func (n *Node) Delete(w http.ResponseWriter, r *http.Request) {
	// Extract key from the query parameters
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	// Remove the key-value pair from the database
	n.mu.Lock()
	delete(n.data, key)
	n.mu.Unlock()

	// Respond to the client
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Deleted key: %s\n", key)
}

func main() {
	// Create a new node instance
	node := NewNode()

	// Define HTTP routes and handlers
	http.HandleFunc("/put", node.Put)
	http.HandleFunc("/get", node.Get)
	http.HandleFunc("/delete", node.Delete)

	// Start the HTTP server
	port := ":8080"
	fmt.Printf("Node running on port %s\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
