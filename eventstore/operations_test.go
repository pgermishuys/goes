package goes

import (
	"log"
	"testing"

	"github.com/satori/go.uuid"
)

func createTestEvent() Event {
	return Event{
		EventID:   uuid.NewV4(),
		EventType: "TestEvent",
		IsJSON:    true,
		Data:      []byte("{}"),
		Metadata:  []byte("{}"),
	}
}

func TestAppendToStream_SingleEvent(t *testing.T) {
	config := &Configuration{
		Address: "127.0.0.1",
		Port:    1113,
	}
	conn, err := NewEventStoreConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	err = conn.Connect()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	defer conn.Close()
	events := []Event{
		createTestEvent(),
	}

	AppendToStream(conn, uuid.NewV4().String(), -2, events)
}

func TestAppendToStream_MultipleEvents(t *testing.T) {
	config := &Configuration{
		Address: "127.0.0.1",
		Port:    1113,
	}
	conn, err := NewEventStoreConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	err = conn.Connect()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	defer conn.Close()
	events := []Event{
		createTestEvent(),
	}
	AppendToStream(conn, uuid.NewV4().String(), -2, events)
}
