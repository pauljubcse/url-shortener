package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

func main() {
	// The server URL
	IPFlag := flag.String("ip", "127.0.0.1", "IP")
	serverIDFlag := flag.String("serverID", "1", "IP")
	portFlag := flag.String("port", ":9000", "Port")
	flag.Parse()
	IP := *IPFlag
	port := *portFlag
	serverID := *serverIDFlag

	serverURL := "ws://localhost:8080/register"

	// Create a dialer
	dialer := websocket.DefaultDialer

	// Set the Client-Real-IP header
	headers := http.Header{}
	headers.Set("client-real-ip", IP)
	headers.Set("client-http-port", port)
	headers.Set("server-id", serverID)
	log.Println(IP)

	// Dial the server and upgrade the connection to WebSocket
	wsConn, _, err := dialer.Dial(serverURL, headers)
	if err != nil {
		log.Fatalf("Failed to upgrade to WebSocket: %v", err)
	}
	defer wsConn.Close()

	go func() {
		_, message, err := wsConn.ReadMessage()
		if err != nil {
			log.Fatalf("Failed to read message: %v", err)
		}
		log.Printf("Received message: %s", message)
	}()

	// Start HTTP server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello from %s", IP+port)
	})

	//port := ":9090" // Define the port you want to use
	fmt.Printf("HTTP server started on %s\n", port)
	log.Fatal(http.ListenAndServe(port, nil))
}
