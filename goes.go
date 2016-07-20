package main

import (
	"log"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

func main() {
	config := &goes.Configuration{
		Address:           "127.0.0.1",
		Port:              1113,
		Login:             "admin",
		Password:          "changeit",
		MaxReconnects:     3,
		ReconnectionDelay: 2000,
	}
	conn, err := goes.NewEventStoreConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	err = conn.Connect()
	defer conn.Close()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	events := []goes.Event{
		goes.Event{
			EventID:   uuid.NewV4(),
			EventType: "itemAdded",
			IsJSON:    true,
			Data:      []byte("{\"price\": \"100\"}"),
			Metadata:  []byte("metadata"),
		},
		goes.Event{
			EventID:   uuid.NewV4(),
			EventType: "itemAdded",
			IsJSON:    true,
			Data:      []byte("{\"price\": \"120\"}"),
			Metadata:  []byte("metadata"),
		},
	}
	go goes.AppendToStream(conn, "shoppingCart-1", -2, events)
	// go goes.ReadSingleEvent(conn, "$stats-127.0.0.1:2113", 0, true, true)
	_, _ = goes.SubscribeToStream(conn, "shoppingCart-1", true, func(evnt *protobuf.StreamEventAppeared) {
		log.Printf("[info] event appeared: %+v\n", evnt)
	}, func(subDropped *protobuf.SubscriptionDropped) {
		log.Printf("[info] subscription dropped\n", subDropped)
	})
	select {}
}
