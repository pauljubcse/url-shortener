package main

import (
	"fmt"
	//"io"
	"flag"
	"net/http"
	"time"
	//"net/url"
	//"sync/atomic"
)

var (
	delay int    = 0
	port  string = ":8080"
)

// handlers
func root(w http.ResponseWriter, req *http.Request) {
	time.Sleep(time.Duration(delay) * time.Second)
	fmt.Fprintf(w, "%s", port)
}
func main() {
	delayFlag := flag.Int("delay", 0, "Delay in seconds before responding")
	portFlag := flag.String("port", "8081", "Port to listen on")
	flag.Parse()
	delay = *delayFlag
	port = *portFlag
	fmt.Println(port)
	http.HandleFunc("/", root)
	err := http.ListenAndServe("localhost:"+port, nil)
	if err != nil {
		fmt.Println(err.Error())
	}
}
