package main

import (
	"log"
	"time"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

func main() {
	config := goes.NewConfiguration()
	config.Address = "127.0.0.1"
	config.Port = 1113
	config.Login = "admin"
	config.Password = "changeit"

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
	persistentSubscription(conn)

	result, err := goes.AppendToStream(conn, "shoppingCart-1", -2, events)
	if *result.Result != protobuf.OperationResult_Success {
		log.Printf("[info] WriteEvents failed. %v", result.Result.String())
	}
	if err != nil {
		log.Printf("[error] WriteEvents failed. %v", err.Error())
	}
	// go goes.ReadSingleEvent(conn, "$stats-127.0.0.1:2113", 0, true, true)
	// subscribe(conn)
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

func persistentSubscription(conn *goes.EventStoreConnection) {
	settings := goes.NewPersistentSubscriptionSettings()
	streamName := "shoppingCart-1"
	groupName := "testGroup"
	_, err := goes.CreatePersistentSubscription(conn, streamName, groupName, *settings)
	if err != nil {
		log.Printf("[error] failed create persistent subscription. %s", err.Error())
		return
	}

	sub, err := goes.ConnectToPersistentSubscription(conn, streamName, groupName, func(evnt *protobuf.PersistentSubscriptionStreamEventAppeared) {
		log.Printf("[info] event appeared. %+v\n", evnt)
	}, func(dropped *protobuf.SubscriptionDropped) {
		log.Printf("[info] subscription dropped. %+v\n", dropped)
	}, 0, true)

	log.Printf("[info] Sub connected. %+v\n", sub)
}
