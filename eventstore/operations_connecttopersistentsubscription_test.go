package goes_test

import (
	"log"
	"testing"
	"time"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

func TestConnectToPersistentSubscription_WhenSubscriptionExists(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	settings := goes.NewPersistentSubscriptionSettings()
	streamID := "testStream"
	groupName := uuid.NewV4().String()
	_, err := goes.CreatePersistentSubscription(conn, streamID, groupName, *settings)
	if err != nil {
		t.Fatalf("Unexpected error creating subscription. %+v", err)
	}

	log.Println("Created persistent subscription")

	eventAppeared := make(chan bool, 1)
	_, err = goes.ConnectToPersistentSubscription(conn, streamID, groupName, func(evnt *protobuf.PersistentSubscriptionStreamEventAppeared) {
		log.Printf("event appeared: %+v\n", evnt)
		eventAppeared <- true
	}, func(subDropped *protobuf.SubscriptionDropped) {
		log.Printf("subscription dropped: %+v\n", subDropped)
	}, 0, true)

	if err != nil {
		t.Fatalf("Unexpected error. %+v", err)
	}

	events := []goes.Event{
		createTestEvent(),
	}
	_, err = goes.AppendToStream(conn, streamID, -2, events)
	if err != nil {
		t.Fatalf("Unexpected failure writing events %+v", err)
	}

	timeout := time.After(3 * time.Second)
	select {
	case <-eventAppeared:
		t.Log("Event appeared")
		break
	case <-timeout:
		t.Fatal("Timed out waiting for event to appear")
		break
	}
}
