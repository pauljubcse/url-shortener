package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/lib/pq"
	"github.com/rabbitmq/amqp091-go"
)

const (
	host     = "localhost"
	dbport   = 5432
	user     = "postgres"
	password = "8184"
	dbname   = "urlsh"
)

type DBInfo struct {
	NumRows int64 `json:"numRows"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type UrlMessage struct {
	ShortUrl string `json:"shortUrl"`
	LongUrl  string `json:"longUrl"`
	GlobalID int64  `json:"globalID"`
}

func queryByShortUrl(db *sql.DB, table string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		shortUrl := r.URL.Query().Get("shortUrl")
		if shortUrl == "" {
			http.Error(w, "Missing shortUrl parameter", http.StatusBadRequest)
			return
		}

		var longUrl string
		query := fmt.Sprintf("SELECT long_url FROM %s WHERE short_url = $1", table)
		err := db.QueryRow(query, shortUrl).Scan(&longUrl)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Short URL not found", http.StatusNotFound)
			} else {
				http.Error(w, "Error querying the database", http.StatusInternalServerError)
				log.Printf("Failed to query database: %s", err)
			}
			return
		}
		fmt.Println(shortUrl)
		fmt.Println(longUrl)
		response := UrlMessage{
			ShortUrl: shortUrl,
			LongUrl:  longUrl,
			GlobalID: 0,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}
func queryByGlobalID(db *sql.DB, table string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		globalID := r.URL.Query().Get("globalID")
		if globalID == "" {
			http.Error(w, "Missing globalID parameter", http.StatusBadRequest)
			return
		}

		globalIDInt, err := strconv.ParseInt(globalID, 10, 64)
		if err != nil {
			http.Error(w, "Invalid globalID parameter", http.StatusBadRequest)
			return
		}

		var shortUrl, longUrl string
		query := fmt.Sprintf("SELECT short_url, long_url FROM %s WHERE global_id = $1", table)
		err = db.QueryRow(query, globalIDInt).Scan(&shortUrl, &longUrl)
		if err != nil {
			if err == sql.ErrNoRows {
				http.Error(w, "Global ID not found", http.StatusNotFound)
			} else {
				http.Error(w, "Error querying the database", http.StatusInternalServerError)
				log.Printf("Failed to query database: %s", err)
			}
			return
		}

		response := UrlMessage{
			ShortUrl: shortUrl,
			LongUrl:  longUrl,
			GlobalID: globalIDInt,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	}
}
func main() {
	shardFlag := flag.Int("shard", 0, "shard id")
	portFlag := flag.String("port", ":10000", "Port")
	flag.Parse()
	//port := *portFlag
	shard := *shardFlag
	table := "urlsh" + strconv.Itoa(int(shard))
	fmt.Println(table)
	port := *portFlag

	// Construct the connection string
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, dbport, user, password, dbname)

	// Open the connection
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error opening database: %q", err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error pinging database: %q", err)
	}
	// Start HTTP server for serving read queries
	http.HandleFunc("/queryByShortUrl", queryByShortUrl(db, table))
	http.HandleFunc("/queryByGlobalID", queryByGlobalID(db, table))

	go func() {
		fmt.Printf("DB server started on %s\n", port)
		log.Fatal(http.ListenAndServe(port, nil))
	}()

	serverURL := "ws://localhost:8080/dbregister"

	// Create a dialer
	dialer := websocket.DefaultDialer

	// Set the Client-Real-IP header
	headers := http.Header{}
	headers.Set("shard-id", strconv.Itoa(int(shard)))
	headers.Set("db-conn-str", psqlInfo)
	headers.Set("table-name", table)
	headers.Set("read-server-port", port)

	fmt.Println(shard)
	// Dial the server and upgrade the connection to WebSocket
	wsConn, _, err := dialer.Dial(serverURL, headers)
	if err != nil {
		log.Fatalf("Failed to upgrade to WebSocket: %v", err)
	}
	defer wsConn.Close()

	go func() {
		for {
			var numRows int
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
			err = db.QueryRow(query).Scan(&numRows)
			if err != nil {
				numRows = -1
			}
			err := wsConn.WriteJSON(DBInfo{
				NumRows: int64(numRows),
			})
			if err != nil {
				log.Fatalf("Failed to send message: %v", err)
			}
			time.Sleep(time.Second * 5)
		}
	}()

	// // Start HTTP server
	// http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
	// 	fmt.Fprintf(w, "Hello from shard: %s", shard)
	// })

	// //Use Port 10000 + shard
	// fmt.Printf("DB server started on %s\n", port)
	// log.Fatal(http.ListenAndServe(port, nil))

	//Connect to RabbitMQ
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// Declare an exchange
	err = ch.ExchangeDeclare(
		"writes", // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare an exchange")

	// Declare a queue for this consumer
	q, err := ch.QueueDeclare(
		strconv.Itoa(int(shard)), // id
		false,                    // durable
		false,                    // delete when unused
		false,                    // exclusive
		false,                    // no-wait
		nil,                      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	// Bind the queue to the exchange with routing keys
	writeKey := fmt.Sprintf("write-%s", strconv.Itoa(int(shard)))
	replicateKey := fmt.Sprintf("replicate-%s", strconv.Itoa(int(shard)))

	err = ch.QueueBind(
		q.Name,   // queue name
		writeKey, // routing key
		"writes", // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind write queue")

	err = ch.QueueBind(
		q.Name,       // queue name
		replicateKey, // routing key
		"writes",     // exchange
		false,
		nil,
	)
	failOnError(err, "Failed to bind replicate queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var urlMessage UrlMessage
			err := json.Unmarshal(d.Body, &urlMessage)
			if err != nil {
				log.Printf("Failed to unmarshal message: %s", err)
				continue
			}
			log.Printf("Received a message: shortUrl=%s, longUrl=%s, globalID=%d", urlMessage.ShortUrl, urlMessage.LongUrl, urlMessage.GlobalID)
			// Process the message (e.g., insert into the database)
			// Insert the received data into the database
			query := fmt.Sprintf("INSERT INTO %s (global_id, short_url, long_url) VALUES ($1, $2, $3)", table)
			_, err = db.Exec(query, urlMessage.GlobalID, urlMessage.ShortUrl, urlMessage.LongUrl)
			if err != nil {
				log.Printf("Failed to insert data into database: %s", err)
			} else {
				log.Printf("Successfully inserted data into database: shortUrl=%s, longUrl=%s, globalID=%d", urlMessage.ShortUrl, urlMessage.LongUrl, urlMessage.GlobalID)
			}
		}
	}()

	log.Printf("Consumer %s waiting for messages. To exit press CTRL+C", strconv.Itoa(int(shard)))
	<-forever
}
