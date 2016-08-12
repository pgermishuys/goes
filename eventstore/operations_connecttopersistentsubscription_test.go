package goes_test

import (
	"log"
	"testing"

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

	result, err := goes.ConnectToPersistentSubscription(conn, streamID, groupName, func(evnt *protobuf.StreamEventAppeared) {
		log.Printf("event appeared: %+v\n", evnt)
	}, func(subDropped *protobuf.SubscriptionDropped) {
		log.Printf("subscription dropped: %+v\n", subDropped)
	}, 0, true)

	log.Printf("Result: %+v\n", result)
	if err != nil {
		t.Fatalf("Unexpected error. %+v", err)
	}
}
