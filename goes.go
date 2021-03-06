package main

import (
	"log"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
)

func main() {
	config := &goes.Configuration{
		ReconnectionDelay:   10000,
		MaxReconnects:       10,
		MaxOperationRetries: 10,
		Address:             "127.0.0.1",
		Port:                1113,
		Login:               "admin",
		Password:            "changeit",
	}

	conn, err := goes.NewEventStoreConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	defer conn.Close()
	err = conn.Connect()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	events := []goes.Event{
		goes.Event{
			EventID:   uuid.Must(uuid.NewV4()),
			EventType: "itemAdded",
			IsJSON:    true,
			Data:      []byte("{\"price\": \"100\"}"),
			Metadata:  []byte("metadata"),
		},
		goes.Event{
			EventID:   uuid.Must(uuid.NewV4()),
			EventType: "itemAdded",
			IsJSON:    true,
			Data:      []byte("{\"price\": \"120\"}"),
			Metadata:  []byte("metadata"),
		},
	}
	result, err := goes.AppendToStream(conn, "shoppingCart-1", 5, events)
	if *result.Result != protobuf.OperationResult_Success {
		log.Printf("[info] WriteEvents failed. %v", result.Result.String())
	}
	if err != nil {
		log.Printf("[error] WriteEvents failed. %v", err.Error())
	}
	// go goes.ReadSingleEvent(conn, "$stats-127.0.0.1:2113", 0, true, true)
	subscribe(conn)
	select {}
}

func subscribe(conn *goes.EventStoreConnection) {
	_, err := goes.SubscribeToStream(conn, "shoppingCart-1", true, func(evnt *protobuf.StreamEventAppeared) {
		log.Printf("[info] event appeared: %+v\n", evnt)
	}, func(subDropped *protobuf.SubscriptionDropped) {
		log.Printf("[info] subscription dropped %+v %+v\n", subDropped, conn)
		time.Sleep(time.Duration(5000) * time.Millisecond)
		subscribe(conn)
	})
	if err != nil {
		log.Printf("[error] failed to start subscription. %s", err.Error())
	}
}
