package main

import (
	"log"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/satori/go.uuid"
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
	events := []eventstore.Event{
		eventstore.Event{
			EventID:   uuid.NewV4(),
			EventType: "itemAdded",
			IsJSON:    true,
			Data:      []byte("{\"price\": \"100\"}"),
			Metadata:  []byte("metadata"),
		},
	}
	go eventstore.AppendToStream(conn, "shoppingCart-1", -2, events)
	go eventstore.ReadSingleEvent(conn, "$stats-127.0.0.1:2113", 0, true, true)
	select {}
}
