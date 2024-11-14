package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
)

type Node struct {
	isParent    bool
	data        map[string]string
	mu          sync.RWMutex
	childNodes  []string
	parentNode  string
	selfAddress string
}

func NewNode(isParent bool, parentNode string, childNodes []string, selfAddress string) *Node {
	return &Node{
		data:        make(map[string]string),
		isParent:    isParent,
		parentNode:  parentNode,
		childNodes:  childNodes,
		selfAddress: selfAddress,
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

// BETWEEN NODES TYPE OF STUFF
func (n *Node) SetParentNode(w http.ResponseWriter, r *http.Request) {
	if n.isParent {
		http.Error(w, "Parent nodes cannot have a parent", http.StatusBadRequest)
		return
	}

	// Parse the new parent node address from the request body
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	newParent, ok := body["parentNode"]
	if !ok || newParent == "" {
		http.Error(w, "Missing 'parentNode' in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.parentNode = newParent
	n.mu.Unlock()

	// Register with the parent node
	err := n.registerWithParent()
	if err != nil {
		http.Error(w, "Failed to register with parent node: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Synchronize data from the parent node
	err = n.synchronizeData()
	if err != nil {
		http.Error(w, "Failed to synchronize data: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Parent node updated to: %s\n", newParent)
}

func (n *Node) registerWithParent() error {
	if n.selfAddress == "" {
		return fmt.Errorf("self address not set")
	}

	registrationData := map[string]string{
		"childNode": n.selfAddress,
	}
	jsonData, _ := json.Marshal(registrationData)

	resp, err := http.Post("http://"+n.parentNode+"/addChild", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("registration failed: %s", string(bodyBytes))
	}

	return nil
}

func (n *Node) synchronizeData() error {
	resp, err := http.Get("http://" + n.parentNode + "/display")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("failed to synchronize data: %s", string(bodyBytes))
	}

	var data map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return err
	}

	n.mu.Lock()
	n.data = data
	n.mu.Unlock()

	return nil
}

func (n *Node) AddChildNode(w http.ResponseWriter, r *http.Request) {
	if !n.isParent {
		http.Error(w, "Only parent nodes can add child nodes", http.StatusBadRequest)
		return
	}

	// Parse the new child node address from the request body
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	newChild, ok := body["childNode"]
	if !ok || newChild == "" {
		http.Error(w, "Missing 'childNode' in request", http.StatusBadRequest)
		return
	}

	n.mu.Lock()
	n.childNodes = append(n.childNodes, newChild)
	n.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "Child node added: %s\n", newChild)
}

func GetSelfAddress(port string) (string, error) {
	// Get the container IP address
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	var ipAddr string
	for _, addr := range addrs {
		// Check the address type and skip loopback interfaces
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() {
			if ipNet.IP.To4() != nil {
				ipAddr = ipNet.IP.String()
				break
			}
		}
	}

	if ipAddr == "" {
		return "", fmt.Errorf("could not determine self IP address")
	}

	return fmt.Sprintf("%s:%s", ipAddr, port), nil
}

// INIT FUNCTION

func main() {
	// Define command-line flags
	isParent := flag.Bool("parent", false, "Set to true if this is the parent node")
	childNodes := flag.String("childNodes", "", "Comma-separated list of child node IPs if this is a parent node")
	port := flag.String("port", "8080", "Port on which the node will run")

	flag.Parse()

	parentNodeEnv := os.Getenv("PARENT_NODE")
	fmt.Println(parentNodeEnv)
	selfAddressEnv := os.Getenv("SELF_ADDRESS")

	// Determine self address
	var selfAddress string
	if selfAddressEnv != "" {
		selfAddress = selfAddressEnv
	} else {
		// Automatically get self address
		addr, err := GetSelfAddress(*port)
		if err != nil {
			log.Fatalf("Failed to get self address: %v", err)
		}
		selfAddress = addr
	}

	// Parse child nodes if provided
	var childNodeList []string
	if *childNodes != "" {
		childNodeList = strings.Split(*childNodes, ",")
	}

	// Create a new node with the provided configuration
	node := NewNode(*isParent, parentNodeEnv, childNodeList, selfAddress)

	if !node.isParent {
		if node.parentNode == "" {
			log.Fatal("Parent node address is not set")
		}

		// Register with the parent node
		err := node.registerWithParent()
		if err != nil {
			log.Fatalf("Failed to register with parent node: %v", err)
		}

		// Synchronize data from the parent node
		err = node.synchronizeData()
		if err != nil {
			log.Fatalf("Failed to synchronize data: %v", err)
		}
	}

	// Define HTTP routes and handlers
	http.HandleFunc("/put", node.Put)
	http.HandleFunc("/get", node.Get)
	http.HandleFunc("/delete", node.Delete)
	http.HandleFunc("/replicate", node.Replicate)
	http.HandleFunc("/display", node.DisplayData)
	http.HandleFunc("/setParent", node.SetParentNode)
	http.HandleFunc("/addChild", node.AddChildNode)

	// Start the HTTP server on the specified port
	fmt.Printf("Node running on port %s (Parent: %v, Parent Node: %s, Child Nodes: %v)\n", *port, *isParent, node.parentNode, childNodeList)
	log.Fatal(http.ListenAndServe(":"+*port, nil))
}
