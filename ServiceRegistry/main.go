package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Setting up Events
// EventType represents the type of event.
type EventType int

// Define event types as constants.
const (
	ServerAddition EventType = iota
	ServerFailure
	ServerMaintenance
	DBAddition
	DBFailure
)

// String returns the string representation of the EventType.
func (et EventType) String() string {
	return [...]string{"ServerAddition", "ServerFailure", "ServerMaintenance", "DBAddition", "DBFailure"}[et]
}

// Event represents an event with a type and additional data.
type Event struct {
	Type      EventType `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"data"`
	ServerID  string    `json:"serverID"`
	ShardID   string    `json:"dbID"`
	DBConnStr string    `json:"dbconnstr"`
}

func NewEvent(eventType EventType, data, serverID, shardID, dbconnstr string) Event {
	return Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
		ServerID:  serverID,
		ShardID:   shardID,
		DBConnStr: dbconnstr,
	}
}

var (
	// Whitelisted IPs map
	whitelistedIPs = map[string]bool{
		"192.168.1.1":  true,
		"192.168.1.2":  true,
		"192.168.1.3":  true,
		"192.168.1.4":  true,
		"192.168.1.5":  true,
		"192.168.1.6":  true,
		"192.168.1.7":  true,
		"192.168.1.8":  true,
		"192.168.1.9":  true,
		"192.168.1.10": true,
	}

	// Services map to hold active connections
	services   = make(map[string]Service)
	dbMap      = make(map[int]DBConn)
	dbs        = make([]DBConn, 0)
	servicesMu sync.Mutex
	dbsMu      sync.Mutex

	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     func(r *http.Request) bool { return true },
	}

	serviceEvents = make(chan Event, 100) //Buffered or Async Channel
	//Will use integer codes
	//1001 -> new Server Addition
)

// Service struct to hold the WebSocket connection and client details
type Service struct {
	Conn           *websocket.Conn
	ClientIP       string
	ClientHTTPIP   string
	clientHTTPPort string
	serverID       string
}

type DBConn struct {
	Connstr  string
	shardID  int
	rowCount int64
	active   bool
}

// RequestBody struct to parse the request body
// type RequestBody struct {
// 	BackendSocketServerAddress string `json:"backend-socket-server-address"`
// }

// findClientIP retrieves the client IP from the Client-Real-IP header
func findClientIP(r *http.Request) string {
	return r.Header.Get("client-real-ip")
}
func findClientPort(r *http.Request) string {
	return r.Header.Get("client-http-port")
}
func findServerID(r *http.Request) string {
	return r.Header.Get("server-id")
}

// isWhitelisted checks if the IP is in the whitelist
func isWhitelisted(ip string) bool {
	// _, ok := whitelistedIPs[ip]
	// return ok
	return true
}

// handleRegister handles the HTTP POST request for registration
func handleRegister(w http.ResponseWriter, r *http.Request) {
	clientIP := findClientIP(r)
	if clientIP == "" {
		http.Error(w, "client-real-ip header not found", http.StatusBadRequest)
		return
	}
	clientHTTPPort := findClientPort(r)
	if clientHTTPPort == "" {
		http.Error(w, "client-http-port header not found", http.StatusBadRequest)
		return
	}
	serverID := findServerID(r)
	if serverID == "" {
		http.Error(w, "server-id header not found", http.StatusBadRequest)
		return
	}

	if !isWhitelisted(clientIP) {
		http.Error(w, "Access Blocked", http.StatusForbidden)
		return
	}

	// Log client IP from client-real-ip header, RemoteAddr, and Origin header
	remoteAddr := r.RemoteAddr
	origin := r.Header.Get("Origin")
	log.Printf("client-real-ip: %s, client-http-port %s, RemoteAddr: %s, Origin: %s, serverID: %s\n", clientIP, clientHTTPPort, remoteAddr, origin, serverID)

	//Upgrade to WebSocket
	backendConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Connection established")
	defer backendConn.Close()
	//Add the new service to the services map
	service := Service{
		Conn:           backendConn,
		ClientIP:       clientIP,
		ClientHTTPIP:   clientIP,
		clientHTTPPort: clientHTTPPort,
		serverID:       serverID,
	}
	servicesMu.Lock()
	services[clientIP] = service
	servicesMu.Unlock()

	//log.Printf("Backend Service %s connected", clientIP)
	serviceEvents <- NewEvent(ServerAddition, service.ClientHTTPIP+service.clientHTTPPort, serverID, "", "") //Send a signal to service Events channel
	//Handle WebSocket connection
	handleWebSocketConnection(service, backendConn)
}

// handleWebSocketConnection handles the WebSocket connection with backendServer and removes it from the services map on disconnection
func handleWebSocketConnection(service Service, backendConn *websocket.Conn) {
	defer func() {
		service.Conn.Close()
		backendConn.Close()
		servicesMu.Lock()
		delete(services, service.ClientIP)
		servicesMu.Unlock()
		//log.Printf("Client %s disconnected", service.ClientIP)
		serviceEvents <- NewEvent(ServerFailure, service.ClientHTTPIP+service.clientHTTPPort, service.serverID, "", "") //Send a signal to service Events channel
	}()
	//blocking operation that does nothing, i just want to keep control in listener till backend server is alive
	_, message, err := backendConn.ReadMessage()
	if err != nil {
		log.Println("Backend read error:", err)
	}
	log.Println(message)

}

// Returns list of available backend servers and upgrades connection to a ws conn
// to send server failure info in real-time
func getServices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}

	//Prepare Services List
	// servicesMu.Lock()
	// // Create a list of client IPs
	// var clientIPs []string
	// for clientIP := range services {
	// 	clientIPs = append(clientIPs, clientIP)
	// }
	// // Convert the list to JSON and send it in the response
	// w.Header().Set("Content-Type", "application/json")
	// err := json.NewEncoder(w).Encode(clientIPs)
	// if err != nil {
	// 	http.Error(w, "Failed to encode JSON", http.StatusInternalServerError)
	// }
	// servicesMu.Unlock()

	// Log client IP from client-real-ip header, RemoteAddr, and Origin header
	// remoteAddr := r.RemoteAddr
	// origin := r.Header.Get("Origin")
	// log.Printf("RemoteAddr: %s, Origin: %s", remoteAddr, origin)

	//Upgrade to WebSocket
	LBConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}
	log.Println("Connection established")
	defer LBConn.Close()

	handleLoadBalancerConnection(LBConn)
}

func handleLoadBalancerConnection(LBConn *websocket.Conn) {
	defer func() {
		LBConn.Close()
		log.Printf("LB disconnected")
	}()

	for {
		event := <-serviceEvents //Listen for events and notify the Load Balancer
		//Notify
		err := sendEvent(LBConn, event)
		if err != nil {
			log.Println("Error Sending Event")
		}
		log.Println(event.Type)
	}

}

// General Purpose Function to send objects or events over websocket
func sendEvent(conn *websocket.Conn, event Event) error {
	// Serialize event to JSON
	eventJSON, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}

	// Send JSON data over WebSocket
	err = conn.WriteMessage(websocket.TextMessage, eventJSON)
	if err != nil {
		return fmt.Errorf("failed to send event over WebSocket: %v", err)
	}

	return nil
}

//	func main() {
//		http.HandleFunc("/register", handleRegister)
//		http.HandleFunc("/getServices", getServices)
//		log.Println("Server started on :8080")
//		err := http.ListenAndServe("192.168.1.9:8080", nil)
//		if err != nil {
//			log.Fatal("ListenAndServe error:", err)
//		}
//	}

func findShardID(r *http.Request) string {
	return r.Header.Get("shard-id")
}
func findDBConnStr(r *http.Request) string {
	return r.Header.Get("db-conn-str")
}

// handle DB events
func handleDBRegister(w http.ResponseWriter, r *http.Request) {
	shardID := findShardID(r)
	if shardID == "" {
		http.Error(w, "shard-id header not found", http.StatusBadRequest)
		return
	}
	shardIDInt, err := strconv.Atoi(shardID)
	if err != nil {
		http.Error(w, "shard-id header not integer", http.StatusBadRequest)
		return
	}

	dbconnstr := findDBConnStr(r)
	if dbconnstr == "" {
		http.Error(w, "db-conn-str not found", http.StatusBadRequest)
		return
	}

	log.Printf("shard-id: %s, db-conn-str %ss\n", shardID, dbconnstr)

	db := DBConn{
		shardID:  shardIDInt,
		Connstr:  dbconnstr,
		rowCount: 0,
		active:   false,
	}
	dbsMu.Lock()
	//dbs = append(dbs, db)
	dbMap[shardIDInt] = db
	dbsMu.Unlock()
}

// monitor DB
func pingDBs() {
	for {
		dbsMu.Lock()
		for shardID, db := range dbMap {
			conn, err := sql.Open("postgres", db.Connstr)
			if err != nil {
				log.Printf("Failed to connect to DB: %v", err)
				db.active = false
				dbMap[shardID] = db
				continue
			}

			err = conn.Ping()
			if err != nil {
				log.Printf("Failed to ping DB: %v", err)
				db.active = false
				dbMap[shardID] = db
				conn.Close()
				continue
			}

			var rowCount int
			err = conn.QueryRow("SELECT COALESCE(MAX(localid), 0) FROM urlsh1").Scan(&rowCount)
			if err != nil {
				log.Printf("Failed to get row count: %v", err)
				db.active = false
				dbMap[shardID] = db
				conn.Close()
				continue
			}

			db.rowCount = rowCount
			db.active = true
			dbMap[shardID] = db
			conn.Close()
		}

		dbs = dbs[:0]
		for _, db := range dbMap {
			if db.Active {
				dbs = append(dbs, db)
			}
		}

		sort.Slice(dbs, func(i, j int) bool {
			return dbs[i].RowCount < dbs[j].RowCount
		})

		dbsMu.Unlock()

		time.Sleep(5 * time.Second)
	}
}

func handleListDBs(w http.ResponseWriter, r *http.Request) {
	dbsMu.Lock()
	defer dbsMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(dbs)
	if err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}
func main() {
	// Define the port flag
	port := flag.String("port", "8080", "port to listen on")
	flag.Parse()

	http.HandleFunc("/register", handleRegister)
	http.HandleFunc("/getServices", getServices)
	address := "localhost:" + *port
	log.Printf("Server started on %s\n", address)
	err := http.ListenAndServe(address, nil)
	if err != nil {
		log.Fatal("ListenAndServe error:", err)
	}
}
