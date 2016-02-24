package main

import (
	// "bytes"
	// "encoding/binary"
	"github.com/pgermishuys/goes/eventstore"
	// "github.com/satori/go.uuid"
	// "io"
	"log"
	// "net"
	// "os"
)

func main() {
	config := &eventstore.Configuration{
		Address: "127.0.0.1",
		Port:    1113,
	}
	conn, err := eventstore.NewConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	err = conn.Connect()
	defer conn.Close()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
}
