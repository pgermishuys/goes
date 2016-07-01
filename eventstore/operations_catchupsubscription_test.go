package goes_test

import (
	"testing"

	"log"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/satori/go.uuid"
)

func TestCatchupSubscription(t *testing.T) {
	conn := createTestConnection(t)
	// defer conn.Close()

	streamID := uuid.NewV4().String()

	events := []goes.Event{
		createTestEvent(),
	}

	_, err := goes.AppendToStream(conn, uuid.NewV4().String(), -2, events)

	_, err = goes.SubscribeToStream(conn, streamID, true, func() {
		log.Printf("[info] event appeared\n")
	})
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
}
