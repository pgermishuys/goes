package goes_test

import (
	"testing"

	uuid "github.com/gofrs/uuid"
	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
)

func TestReadSinglEvent_WithNoEventsInStream(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	result, err := goes.ReadSingleEvent(conn, uuid.Must(uuid.NewV4()).String(), 0, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.ReadEventCompleted_NoStream
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
}

func TestReadSinglEvent_WithEventsInStream(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	streamID := uuid.Must(uuid.NewV4()).String()
	eventID := uuid.Must(uuid.NewV4())
	events := []goes.Event{
		goes.Event{
			EventID:   eventID,
			EventType: "TestEvent",
			IsJSON:    true,
			Data:      []byte("{}"),
			Metadata:  []byte("{}"),
		},
	}

	goes.AppendToStream(conn, streamID, -2, events)

	result, err := goes.ReadSingleEvent(conn, streamID, 0, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.ReadEventCompleted_Success
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
	gotEventID, _ := uuid.FromBytes(result.GetEvent().GetEvent().GetEventId())
	if gotEventID != eventID {
		t.Fatalf("Expected %v got %v", eventID, gotEventID)
	}
}

func TestReadSingleEvent_WithInvalidCredentials(t *testing.T) {
	conn := createTestConnection(t)

	streamID := uuid.Must(uuid.NewV4()).String()
	eventID := uuid.Must(uuid.NewV4())
	events := []goes.Event{
		goes.Event{
			EventID:   eventID,
			EventType: "TestEvent",
			IsJSON:    true,
			Data:      []byte("{}"),
			Metadata:  []byte("{}"),
		},
	}

	goes.AppendToStream(conn, streamID, -2, events)
	conn.Close()

	conn = createTestConnection(t)
	defer conn.Close()
	conn.Config.Login = "BadUser"
	conn.Config.Password = "Pass"

	_, err := goes.ReadSingleEvent(conn, "$all", 0, true, true)
	if err == nil {
		t.Fatalf("Expected failure")
	}
	expectedError := notAuthenticatedError
	if err.Error() != expectedError {
		t.Fatalf("Expected %s got %s", expectedError, err.Error())
	}
}
