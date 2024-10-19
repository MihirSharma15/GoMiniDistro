package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
)

type Node struct {
	isParent bool
	data     map[string]string
	mu       sync.RWMutex

	childNodes []string
	parentNode string
}

func NewNode() *Node {
	return &Node{
		data:     make(map[string]string),
		isParent: false,
	}
}

func (n *Node) Put(w http.ResponseWriter, r *http.Request) {

	if !n.isParent {
		// Redirect to the parent node
		http.Redirect(w, r, "http://"+n.parentNode+"/put", http.StatusTemporaryRedirect)
		return
	}

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

func (n *Node) Delete(w http.ResponseWriter, r *http.Request) {
	if !n.isParent {
		http.Redirect(w, r, "http://"+n.parentNode+"/delete?"+r.URL.RawQuery, http.StatusTemporaryRedirect)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	delete(n.data, key)
	n.mu.Unlock()

	// Replicate deletion to child nodes
	n.replicateDeletionToChildren(key)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Deleted key: %s\n", key)
}

func (n *Node) replicateDeletionToChildren(key string) {
	for _, childAddr := range n.childNodes {
		go func(addr string) {
			deletionData := map[string]string{"key": key}
			jsonData, _ := json.Marshal(deletionData)
			resp, err := http.Post("http://"+addr+"/delete", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Failed to replicate deletion to %s: %v", addr, err)
				return
			}
			resp.Body.Close()
		}(childAddr)
	}
}

func (n *Node) replicateToChildren(key, value string) {
	for _, childAddr := range n.childNodes {
		go func(addr string) {
			replicationData := map[string]string{"key": key, "value": value}
			jsonData, _ := json.Marshal(replicationData)
			resp, err := http.Post("http://"+addr+"/replicate", "application/json", bytes.NewBuffer(jsonData))
			if err != nil {
				log.Printf("Failed to replicate to %s: %v", addr, err)
				return
			}
			resp.Body.Close()
		}(childAddr)
	}
}

func (n *Node) Replicate(w http.ResponseWriter, r *http.Request) {
	if n.isParent {
		http.Error(w, "Parent node cannot receive replication data", http.StatusBadRequest)
		return
	}

	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid replication payload", http.StatusBadRequest)
		return
	}

	key, keyOk := body["key"]
	value, valueOk := body["value"]
	if !keyOk || !valueOk {
		http.Error(w, "Missing key or value in replication data", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.data[key] = value
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Replicated: %s -> %s\n", key, value)
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
