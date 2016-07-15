package goes_test

import (
	"testing"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

const (
	notAuthenticatedError string = "Not Authenticated"
)

func createTestConnection(t *testing.T) *goes.EventStoreConnection {
	config := &goes.Configuration{
		Address:  "127.0.0.1",
		Port:     1113,
		Login:    "admin",
		Password: "changeit",
	}
	conn, err := goes.NewEventStoreConnection(config)
	if err != nil {
		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
	}
	err = conn.Connect()
	if err != nil {
		t.Fatalf("Unexpected failure connecting: %s", err.Error())
	}
	return conn
}

func createTestEvent() goes.Event {
	return goes.Event{
		EventID:   uuid.NewV4(),
		EventType: "TestEvent",
		IsJSON:    true,
		Data:      []byte("{}"),
		Metadata:  []byte("{}"),
	}
}

func TestAppendToStream_SingleEvent(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	events := []goes.Event{
		createTestEvent(),
	}

	result, err := goes.AppendToStream(conn, uuid.NewV4().String(), -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
	expectedLastEventNumber := int32(0)
	if result.GetLastEventNumber() != expectedLastEventNumber {
		t.Fatalf("Expected %d got %d", expectedLastEventNumber, result.GetLastEventNumber())
	}
}

func TestAppendToStream_MultipleEvents(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	events := []goes.Event{
		createTestEvent(),
		createTestEvent(),
	}

	result, err := goes.AppendToStream(conn, uuid.NewV4().String(), -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
	expectedLastEventNumber := int32(1)
	if result.GetLastEventNumber() != expectedLastEventNumber {
		t.Fatalf("Expected %d got %d", expectedLastEventNumber, result.GetLastEventNumber())
	}
}

func TestAppendToStream_WithInvalidExpectedVersion(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	events := []goes.Event{
		createTestEvent(),
	}

	result, err := goes.AppendToStream(conn, uuid.NewV4().String(), 0, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_WrongExpectedVersion
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
}

func TestAppendToSystemStream_WithIncorrectCredentials(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	conn.Config.Login = "BadUser"
	conn.Config.Password = "Pass"
	events := []goes.Event{
		createTestEvent(),
	}

	_, err := goes.AppendToStream(conn, "$"+uuid.NewV4().String(), 0, events)

	if err == nil {
		t.Fatalf("Expected failure")
	}
	expectedError := notAuthenticatedError
	if err.Error() != expectedError {
		t.Fatalf("Expected %s got %s", expectedError, err.Error())
	}
}
