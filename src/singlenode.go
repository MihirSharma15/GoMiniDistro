package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
)

type Node struct {
	isParent   bool
	data       map[string]string
	mu         sync.RWMutex
	childNodes []string
	parentNode string
}

func NewNode(isParent bool, parentNode string, childNodes []string) *Node {
	return &Node{
		data:       make(map[string]string),
		isParent:   isParent,
		parentNode: parentNode,
		childNodes: childNodes,
	}
}

func (n *Node) Put(w http.ResponseWriter, r *http.Request) {

	if !n.isParent {
		if n.parentNode == "" {
			http.Error(w, "Parent node not available", http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "http://"+n.parentNode+"/put", http.StatusTemporaryRedirect)
		return
	}

	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	defer r.Body.Close()

	key, keyOk := body["key"]
	value, valueOk := body["value"]
	if !keyOk || !valueOk {
		http.Error(w, "Missing key or value in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.data[key] = value
	n.mu.Unlock()

	n.replicateToChildren(key, value)

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
	// Ensure the request uses the DELETE method
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key in request", http.StatusBadRequest)
		return
	}

	isReplication := r.Header.Get("X-Replication") == "true"

	if !n.isParent {
		if !isReplication {
			// Redirect client-initiated delete requests to the parent node
			if n.parentNode == "" {
				http.Error(w, "Parent node not available", http.StatusInternalServerError)
				return
			}
			http.Redirect(w, r, "http://"+n.parentNode+"/delete?key="+key, http.StatusTemporaryRedirect)
			return
		} else {
			// Process replication delete request from parent node
			n.mu.Lock()
			delete(n.data, key)
			n.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, "Replicated deletion of key: %s\n", key)
			return
		}
	}

	// This is the parent node handling a client-initiated delete request
	// Proceed to delete the key and replicate to children
	n.mu.Lock()
	delete(n.data, key)
	n.mu.Unlock()

	// Replicate the deletion to child nodes
	n.replicateDeletionToChildren(key)

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Deleted key: %s\n", key)
}

func (n *Node) replicateDeletionToChildren(key string) {
	for _, childAddr := range n.childNodes {
		go func(addr string) {
			// Create a DELETE request with the replication header
			req, err := http.NewRequest(http.MethodDelete, "http://"+addr+"/delete?key="+key, nil)
			if err != nil {
				log.Printf("Failed to create DELETE request for %s: %v", addr, err)
				return
			}
			req.Header.Set("X-Replication", "true") // Mark as a replication request

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Failed to replicate deletion to %s: %v", addr, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf("Replication to %s failed with status: %s", addr, resp.Status)
			}
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
				resp.Body.Close()
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

func (n *Node) DisplayData(w http.ResponseWriter, r *http.Request) {

	n.mu.RLock()
	defer n.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")

	jsonData, err := json.Marshal(n.data)
	if err != nil {
		http.Error(w, "Error encoding data to JSON", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(jsonData)

}

func main() {
	// Define command-line flags
	isParent := flag.Bool("parent", false, "Set to true if this is the parent node")
	parentNode := flag.String("parentNode", "", "IP address of the parent node if this is a child node")
	childNodes := flag.String("childNodes", "", "Comma-separated list of child node IPs if this is a parent node")
	port := flag.String("port", "8080", "Port on which the node will run")

	flag.Parse()

	// Parse child nodes if provided
	var childNodeList []string
	if *childNodes != "" {
		childNodeList = strings.Split(*childNodes, ",")
	}

	// Create a new node with the provided configuration
	node := NewNode(*isParent, *parentNode, childNodeList)

	// Define HTTP routes and handlers
	http.HandleFunc("/put", node.Put)
	http.HandleFunc("/get", node.Get)
	http.HandleFunc("/delete", node.Delete)
	http.HandleFunc("/replicate", node.Replicate)
	http.HandleFunc("/display", node.DisplayData)

	// Start the HTTP server on the specified port
	fmt.Printf("Node running on port %s (Parent: %v, Parent Node: %s, Child Nodes: %v)\n", *port, *isParent, *parentNode, childNodeList)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
