package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pauljubcse/consistent_hashing"
	"github.com/pauljubcse/in_memory_cache"
	"github.com/pauljubcse/kvsclient"
	"github.com/pauljubcse/ratelimiter"
	//"net/url"
	//"sync/atomic"
)

type EventType int

// Define event types as constants.
const (
	ServerAddition EventType = iota
	ServerFailure
	ServerMaintenance
)

type UrlResponse struct {
	LongUrl  string `json:"longUrl"`
	ShortUrl string `json:"shortUrl"`
	GloablID int64  `json:"globalID"`
}

// String returns the string representation of the EventType.
func (et EventType) String() string {
	return [...]string{"ServerAddition", "ServerFailure", "ServerMaintenance"}[et]
}

// Event represents an event with a type and additional data.
type Event struct {
	Type      EventType `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"data"`
	ServerID  string    `json:"serverID"`
}

func NewEvent(eventType EventType, data string) Event {
	return Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
	}
}

// var (
// 	backends = []string{
// 		"http://localhost:8081",
// 		"http://localhost:8082",
// 		"http://localhost:8083",
// 	}
// 	current            uint32
// 	backendTimeout     int
// 	readTimeout        int
// 	writeTimeout       int //Must be more than backend timeout
// 	useServiceRegistry bool
// )

//	func getNextBackend() string {
//		current = (current + 1) % (uint32(len(backends)))
//		return backends[current]
//	}
// var (
// 	// backends = []string{
// 	// 	"http://localhost:8081",
// 	// 	"http://localhost:8082",
// 	// 	"http://localhost:8083",
// 	// }
// 	backends           = []string{}
// 	consistentHash     *consistent_hashing.ConsistentHash
// 	backendTimeout     = 5 // example timeout values
// 	readTimeout        = 5
// 	writeTimeout       = 10
// 	useServiceRegistry = false // example flag
// )

//	func init() {
//		consistentHash = consistent_hashing.NewConsistentHash(3)
//		for _, backend := range backends {
//			consistentHash.AddNode(backend)
//		}
//	}
var lb *LoadBalancer

type LoadBalancer struct {
	backends           []string
	consistentHash     *consistent_hashing.ConsistentHash
	backendTimeout     int
	readTimeout        int
	writeTimeout       int
	useServiceRegistry bool
}

func (lb *LoadBalancer) addServer(key string) {
	lb.consistentHash.AddNode(key)
	fmt.Println("CH Addition")
}
func (lb *LoadBalancer) deleteServer(key string) {
	lb.consistentHash.RemoveNode(key)
	fmt.Println("CH Deletion")
}
func (lb *LoadBalancer) getNextBackend(key string) (string, error) {
	node, found := lb.consistentHash.GetNode(key)
	if !found {
		//log.Fatalf("No backend found for key: %s", key)
		return "", fmt.Errorf("no backend found")
	}
	return node, nil
}


var cache *in_memory_cache.LRUCache[string, string]
var kvsClient *kvsclient.Client
var rateLimiter *ratelimiter.TokenBucket

// handlers
func root(w http.ResponseWriter, req *http.Request) { //Worker Go Routine
	//Create a Http Client
	relay := &http.Client{
		Timeout: time.Duration(lb.backendTimeout) * time.Second,
	}
	//Get a backend server
	backend, err := lb.getNextBackend(req.RemoteAddr)
	if err != nil {
		http.Error(w, "Failed to get backend", http.StatusInternalServerError)
		return
	}
	fmt.Println(backend)
	backendReq, err := http.NewRequest(req.Method, "http://"+backend, req.Body)
	if err != nil {
		fmt.Print(err)
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}
	// Copy headers from the original request to the new request
	for header, values := range req.Header {
		for _, value := range values {
			backendReq.Header.Add(header, value)
		}
	}

	// Perform the request to the backend server
	resp, err := relay.Do(backendReq)
	if err != nil {
		fmt.Fprintf(w, "%s", "Backend Didn't Respond in Time")
		return
	}
	defer resp.Body.Close()

	// Write the status code and response body to the client
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		http.Error(w, "Failed to get Response", http.StatusInternalServerError)
	}
}

func postLongUrl(w http.ResponseWriter, req *http.Request) {
	// Create an HTTP client with a timeout
	relay := &http.Client{
		Timeout: time.Duration(lb.backendTimeout) * time.Second,
	}

	// Get a backend server
	backend, err := lb.getNextBackend(req.RemoteAddr)
	if err != nil {
		http.Error(w, "Failed to get backend", http.StatusInternalServerError)
		return
	}
	fmt.Println("Selected backend:", backend)

	// Parse the longUrl query parameter from the incoming request
	longUrl := req.URL.Query().Get("longUrl")
	if longUrl == "" {
		http.Error(w, "Missing longUrl parameter at Load Balancer", http.StatusBadRequest)
		return
	}

	// Create a new request for the backend server
	backendURL := fmt.Sprintf("http://%s/post?longUrl=%s", backend, longUrl)
	fmt.Println(backendURL)
	backendReq, err := http.NewRequest(req.Method, backendURL, req.Body)
	if err != nil {
		fmt.Println("Error creating backend request:", err)
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}

	// Copy headers from the original request to the new request
	for header, values := range req.Header {
		for _, value := range values {
			backendReq.Header.Add(header, value)
		}
	}

	// Perform the request to the backend server
	resp, err := relay.Do(backendReq)
	if err != nil {
		fmt.Println("Error performing backend request:", err)
		http.Error(w, "Backend didn't respond in time", http.StatusGatewayTimeout)
		return
	}
	defer resp.Body.Close()

	// Write the status code and response body to the client
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	if err != nil {
		http.Error(w, "Failed to get response", http.StatusInternalServerError)
	}
}

// func getLongUrl(w http.ResponseWriter, req *http.Request) {
// 	// Create an HTTP client with a timeout
// 	relay := &http.Client{
// 		Timeout: time.Duration(lb.backendTimeout) * time.Second,
// 	}

// 	// Get a backend server
// 	backend, err := lb.getNextBackend(req.RemoteAddr)
// 	if err != nil {
// 		http.Error(w, "Failed to get backend", http.StatusInternalServerError)
// 		return
// 	}
// 	fmt.Println("Selected backend:", backend)

// 	// Parse the shortUrl query parameter from the incoming request
// 	shortUrl := req.URL.Query().Get("shortUrl")
// 	if shortUrl == "" {
// 		http.Error(w, "Missing shortUrl parameter at LB", http.StatusBadRequest)
// 		return
// 	}

// 	// Create a new request for the backend server
// 	backendURL := fmt.Sprintf("http://%s/query?shortUrl=%s", backend, shortUrl)
// 	backendReq, err := http.NewRequest(http.MethodGet, backendURL, nil)
// 	if err != nil {
// 		fmt.Println("Error creating backend request:", err)
// 		http.Error(w, "Failed to create request", http.StatusInternalServerError)
// 		return
// 	}

// 	// Copy headers from the original request to the new request
// 	for header, values := range req.Header {
// 		for _, value := range values {
// 			backendReq.Header.Add(header, value)
// 		}
// 	}

// 	// Perform the request to the backend server
// 	resp, err := relay.Do(backendReq)
// 	if err != nil {
// 		fmt.Println("Error performing backend request:", err)
// 		http.Error(w, "Backend didn't respond in time", http.StatusGatewayTimeout)
// 		return
// 	}
// 	defer resp.Body.Close()

// 	if resp.StatusCode != http.StatusOK {
// 		http.Error(w, "Failed to get long URL", resp.StatusCode)
// 		return
// 	}

// 	body, err := ioutil.ReadAll(resp.Body)
// 	if err != nil {
// 		http.Error(w, "Failed to read response", http.StatusInternalServerError)
// 		return
// 	}

// 	var urlResp UrlResponse
// 	err = json.Unmarshal(body, &urlResp)
// 	if err != nil {
// 		http.Error(w, "Failed to unmarshal response", http.StatusInternalServerError)
// 		return
// 	}

// 	// Redirect to the long URL
// 	http.Redirect(w, req, urlResp.LongUrl, http.StatusFound)
// }

func getLongUrl(w http.ResponseWriter, req *http.Request) {
	//Rate Limiting on IP
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		fmt.Println("Error getting IP address:", err)
	}
	fmt.Println(ip)
	if !rateLimiter.Allow(ip) {
		fmt.Println("Rate limit exceeded for IP:", req.RemoteAddr)
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
	}
	// Extract shortUrl from the URL path
	shortUrl := strings.TrimPrefix(req.URL.Path, "/")
	// Parse the shortUrl query parameter from the incoming request
	if shortUrl == "" {
		http.Error(w, "Missing shortUrl in path at LB", http.StatusBadRequest)
		return
	}

	cacheValue, found := cache.Get(shortUrl)
	if found {
		fmt.Println("Cache hit for shortUrl:", shortUrl)
		http.Redirect(w, req, cacheValue, http.StatusFound)
		return
	} else {
		fmt.Println("Cache miss for shortUrl:", shortUrl)
	}

	
	// Create an HTTP client with a timeout
	relay := &http.Client{
		Timeout: time.Duration(lb.backendTimeout) * time.Second,
	}

	// Get a backend server
	backend, err := lb.getNextBackend(req.RemoteAddr)
	if err != nil {
		http.Error(w, "Failed to get backend", http.StatusInternalServerError)
		return
	}
	fmt.Println("Selected backend:", backend)

	// Create a new request for the backend server
	backendURL := fmt.Sprintf("http://%s/query?shortUrl=%s", backend, shortUrl)
	backendReq, err := http.NewRequest(http.MethodGet, backendURL, nil)
	if err != nil {
		fmt.Println("Error creating backend request:", err)
		http.Error(w, "Failed to create request", http.StatusInternalServerError)
		return
	}

	// Copy headers from the original request to the new request
	for header, values := range req.Header {
		for _, value := range values {
			backendReq.Header.Add(header, value)
		}
	}

	// Perform the request to the backend server
	resp, err := relay.Do(backendReq)
	if err != nil {
		fmt.Println("Error performing backend request:", err)
		http.Error(w, "Backend didn't respond in time", http.StatusGatewayTimeout)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		http.Error(w, "Failed to get long URL", resp.StatusCode)
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		http.Error(w, "Failed to read response", http.StatusInternalServerError)
		return
	}

	var urlResp UrlResponse
	err = json.Unmarshal(body, &urlResp)
	if err != nil {
		http.Error(w, "Failed to unmarshal response", http.StatusInternalServerError)
		return
	}

	cache.Put(shortUrl, urlResp.LongUrl) // Cache the long URL
	// Redirect to the long URL
	http.Redirect(w, req, urlResp.LongUrl, http.StatusFound)
}

// general purpose function for receiving events via websocket
func receiveEvent(conn *websocket.Conn) (Event, error) {
	var event Event
	// Read message from WebSocket connection
	_, eventJSON, err := conn.ReadMessage()
	if err != nil {
		return event, fmt.Errorf("failed to read message from WebSocket: %v", err)
	}

	// Deserialize JSON data into event struct
	err = json.Unmarshal(eventJSON, &event)
	if err != nil {
		return event, fmt.Errorf("failed to unmarshal event JSON: %v", err)
	}

	return event, nil
}

func connectServiceRegistry() {
	serviceRegistryURL := "ws://localhost:8080/getServices"
	// Create a dialer
	dialer := websocket.DefaultDialer

	// Set the Client-Real-IP header
	headers := http.Header{}
	// Dial the server and upgrade the connection to WebSocket
	serviceRegistryConn, _, err := dialer.Dial(serviceRegistryURL, headers)
	if err != nil {
		log.Fatalf("Failed to upgrade to WebSocket: %v", err)
	}
	defer serviceRegistryConn.Close()

	// // Send a message to the server
	// err = wsConn.WriteMessage(websocket.TextMessage, []byte("Hello, Server!"))
	// if err != nil {
	// 	log.Fatalf("Failed to send message: %v", err)
	// }

	// // Read a message from the server
	for {
		event, err := receiveEvent(serviceRegistryConn)
		if err != nil {
			log.Println(err)
			return
		}
		fmt.Printf("%s\t%s\t%s\n", event.Type.String(), event.Data, event.ServerID) //Server Addition: Event Data is remoteAddr
		if event.Type == ServerAddition {
			lb.addServer(event.Data)
		}
		if event.Type == ServerFailure {
			lb.deleteServer(event.Data)
		}
	}

}

func main() {
	backendTimeoutFlag := flag.Int("backendTimeout", 5, "Time within which backend servers must respond.")
	readTimeoutFlag := flag.Int("readTimeout", 5, "Time within which request header must be read.")
	writeTimeoutFlag := flag.Int("writeTimeout", 5, "Time within which writing response must complete, max lifetime of http.Serve.")
	useServiceRegistryFlag := flag.Bool("useServiceRegistry", false, "To use service registry or not")
	flag.Parse()

	lb = &LoadBalancer{
		backendTimeout:     *backendTimeoutFlag,
		readTimeout:        *readTimeoutFlag,
		writeTimeout:       *writeTimeoutFlag,
		useServiceRegistry: *useServiceRegistryFlag,
	}

	//Connect to Service Registry
	if lb.useServiceRegistry {
		lb.backends = []string{}
		go connectServiceRegistry()
		time.Sleep(time.Second * 2)
	}
	lb.consistentHash = consistent_hashing.NewConsistentHash(3)
	for _, backend := range lb.backends {
		lb.consistentHash.AddNode(backend)
	}

	cache = in_memory_cache.NewLRUCache[string, string](1000) // 1000 is the cache size
	kvsClient, err := kvsclient.NewClient("ws://localhost:9080/ws")
	if err != nil {
		log.Fatalf("Failed to create KVS client: %v", err)	
	}

	// Ping and Check KV Store and Client
	domain := "test_domain"
	err = kvsClient.CreateDomain(domain)
	if err != nil {
		log.Fatalf("Error creating domain: %v", err)
	}
	key := "counter"
	initialValue := "0"
	err = kvsClient.SetString(domain, key, initialValue)
	if err != nil {
		log.Fatalf("Error setting string: %v", err)
	}else{
		fmt.Println("Client and Store Running")
	}

	// Set up a rate limiter with a limit of 10 requests per second
	rl := ratelimiter.NewTokenBucket(kvsClient, domain, 10, 1) // 10 sec interval, 1 token per interval
	fmt.Println("Trial 1 for Rate Limiter:", rl.Allow("1")) // Allow 1 token for the first request
	fmt.Println("Trial 2 for Rate Limiter:", rl.Allow("1")) // Allow 1 token for the first request
	fmt.Println("Trial 3 for Rate Limiter:", rl.Allow("2")) // Allow 1 token for the first request
	//Expect True False True

	rateLimiter = ratelimiter.NewTokenBucket(kvsClient, domain, 10, 2) // 20 sec interval, 1 token per interval
		
	//to relay requests to backend servers
	http.HandleFunc("/hello", root)
	http.HandleFunc("/postLongUrl", postLongUrl)
	http.HandleFunc("/", getLongUrl)
	srv := &http.Server{
		Addr:         ":8090",
		ReadTimeout:  time.Duration(lb.readTimeout) * time.Second,
		WriteTimeout: time.Duration(lb.writeTimeout) * time.Second,
	}
	srv.ListenAndServe()
}
