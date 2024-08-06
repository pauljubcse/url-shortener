package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rabbitmq/amqp091-go"
)

// Constants for the bit lengths of each component
const (
	timestampBits = 41
	machineIDBits = 10
	sequenceBits  = 12

	maxMachineID = (1 << machineIDBits) - 1
	maxSequence  = (1 << sequenceBits) - 1
	epoch        = int64(1288834974657) // Custom epoch (Twitter's Snowflake epoch)

	timestampShift = machineIDBits + sequenceBits
	machineIDShift = sequenceBits
)

// IDGenerator is responsible for generating unique IDs
type IDGenerator struct {
	mu        sync.Mutex
	lastTime  int64
	sequence  int64
	machineID int64
}

// NewIDGenerator creates a new IDGenerator with the given machine ID
func NewIDGenerator(machineID int64) *IDGenerator {
	if machineID < 0 || machineID > maxMachineID {
		panic(fmt.Sprintf("machine ID must be between 0 and %d", maxMachineID))
	}
	return &IDGenerator{
		machineID: machineID,
	}
}

// GenerateID generates a unique ID based on the current timestamp, machine ID, and sequence number
func (gen *IDGenerator) GenerateID() uint64 {
	gen.mu.Lock()
	defer gen.mu.Unlock()

	now := time.Now().UnixMilli()

	if now == gen.lastTime {
		gen.sequence = (gen.sequence + 1) & maxSequence
		if gen.sequence == 0 {
			// Sequence rollover in the same millisecond, wait for the next millisecond
			for now <= gen.lastTime {
				now = time.Now().UnixMilli()
			}
		}
	} else {
		gen.sequence = 0
	}

	gen.lastTime = now

	id := (uint64(now-epoch) << timestampShift) |
		(uint64(gen.machineID) << machineIDShift) |
		uint64(gen.sequence)

	return id
}

// GetMachineID extracts the machine ID from the given ID
func GetMachineID(id uint64) int64 {
	return int64((id >> machineIDShift) & uint64(maxMachineID))
}
func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type DBShardID struct {
	ID int `json:"id"`
}
type UrlMessage struct {
	ShortUrl string `json:"shortUrl"`
	LongUrl  string `json:"longUrl"`
	GlobalID int64  `json:"globalID"`
}

const base62Charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func EncodeBase62(num int64) string {
	if num == 0 {
		return string(base62Charset[0])
	}
	result := ""
	for num > 0 {
		result = string(base62Charset[num%62]) + result
		num /= 62
	}
	return result
}
func main() {
	// The server URL
	IPFlag := flag.String("ip", "127.0.0.1", "IP")
	serverIDFlag := flag.Int64("serverID", 1, "IP")
	portFlag := flag.String("port", ":9000", "Port")
	flag.Parse()
	IP := *IPFlag
	port := *portFlag
	serverID := *serverIDFlag
	shardID := DBShardID{ID: -1}
	IDGen := NewIDGenerator(serverID)
	//Publisher
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare an exchange
	err = ch.ExchangeDeclare(
		"example", // name
		"direct",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	//Connect to Service Registry
	serverURL := "ws://localhost:8080/register"

	// Create a dialer
	dialer := websocket.DefaultDialer

	// Set the Client-Real-IP header
	headers := http.Header{}
	headers.Set("client-real-ip", IP)
	headers.Set("client-http-port", port)
	headers.Set("server-id", strconv.Itoa(int(serverID)))
	log.Println(IP)

	// Dial the server and upgrade the connection to WebSocket
	wsConn, _, err := dialer.Dial(serverURL, headers)
	if err != nil {
		log.Fatalf("Failed to upgrade to WebSocket: %v", err)
	}
	defer wsConn.Close()

	//Receive updates on least filled shard
	go func() {
		for {
			//shardID = DBShardID{ID: -1}
			err := wsConn.ReadJSON(&shardID)
			if err != nil {
				log.Fatalf("Failed to read message: %v", err)
			}
			log.Printf("Received ShardID: %d", shardID.ID)
			time.Sleep(time.Second * 5)
		}
	}()

	// Start HTTP server
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello from %s", IP+port)
		longUrl := r.Header.Get("longUrl")
		fmt.Println(longUrl)
		if longUrl == "" {
			http.Error(w, "Missing longUrl parameter", http.StatusBadRequest)
			return
		}

		shardIDCopy := shardID.ID
		fmt.Println("Generating ID")
		generatedID := IDGen.GenerateID()
		fmt.Println("Generated ID")
		shortUrl := EncodeBase62(int64(shardIDCopy)) + EncodeBase62(int64(generatedID)) //First Letter Shard ID for Routing to Shard
		fmt.Println("ShortURl: ", shortUrl)
		message := UrlMessage{
			ShortUrl: shortUrl,
			LongUrl:  longUrl,
			GlobalID: int64(generatedID),
		}

		messageBody, err := json.Marshal(message)
		if err != nil {
			failOnError(err, "Failed to marshal message")
		}

		err = ch.Publish(
			"writes",                            // exchange
			fmt.Sprintf("write-%d", shardID.ID), // routing key
			false,                               // mandatory
			false,                               // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        messageBody,
			})
		failOnError(err, fmt.Sprintf("Failed to publish write message to consumer %d", shardID.ID))
		log.Printf("Sent: %s", messageBody)

		// Replication message to consumer i+1
		nextConsumer := shardID.ID + 1
		err = ch.Publish(
			"writes", // exchange
			fmt.Sprintf("replicate-%d", nextConsumer), // routing key
			false, // mandatory
			false, // immediate
			amqp091.Publishing{
				ContentType: "application/json",
				Body:        messageBody,
			})
		failOnError(err, fmt.Sprintf("Failed to publish replication message to consumer %d", nextConsumer))
		log.Printf("Sent: %s", messageBody)

		fmt.Fprintf(w, "Short URL: %s\nLong URL: %s\nTimestamp: %d", shortUrl, longUrl, generatedID)
	})

	//port := ":9090" // Define the port you want to use
	fmt.Printf("HTTP server started on %s\n", port)
	log.Fatal(http.ListenAndServe(port, nil))

}
