package goes_test

import (
	"testing"

	uuid "github.com/gofrs/uuid"
	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
)

func TestReadStreamEventsBackward_WithEmptyStream(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	result, err := goes.ReadStreamEventsBackward(conn, uuid.Must(uuid.NewV4()).String(), 0, 1, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.ReadStreamEventsCompleted_NoStream
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
}

func TestReadStreamEventsBackward_WithStreamContainingEvents(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	streamID := uuid.Must(uuid.NewV4()).String()
	events := []goes.Event{
		createTestEvent(),
		createTestEvent(),
	}

	result, err := goes.AppendToStream(conn, streamID, -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.Result)
	}

	readResult, err := goes.ReadStreamEventsBackward(conn, streamID, -1, 2, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedReadResult := protobuf.ReadStreamEventsCompleted_Success
	if readResult.GetResult() != expectedReadResult {
		t.Fatalf("Expected %s got %s", expectedReadResult, readResult.GetResult())
	}
	if len(readResult.Events) != 2 {
		t.Fatalf("Expected %d got %d", 2, len(readResult.Events))
	}
}

func TestReadStreamEventsBackward_WithReadingMoreEventsThanExistsInStream(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	streamID := uuid.Must(uuid.NewV4()).String()
	events := []goes.Event{
		createTestEvent(),
	}

	result, err := goes.AppendToStream(conn, streamID, -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.Result)
	}

	readResult, err := goes.ReadStreamEventsBackward(conn, streamID, -1, 2, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedReadResult := protobuf.ReadStreamEventsCompleted_Success
	if readResult.GetResult() != expectedReadResult {
		t.Fatalf("Expected %s got %s", expectedReadResult, readResult.GetResult())
	}
	if len(readResult.Events) != 1 {
		t.Fatalf("Expected %d got %d", 1, len(readResult.Events))
	}
}

func TestReadStreamEventsBackward_WithInvalidCredentials(t *testing.T) {
	conn := createTestConnection(t)

	streamID := uuid.Must(uuid.NewV4()).String()
	events := []goes.Event{
		createTestEvent(),
	}

	result, err := goes.AppendToStream(conn, streamID, -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.Result)
	}
	conn.Close()

	conn = createTestConnection(t)
	defer conn.Close()
	conn.Config.Login = "BadUser"
	conn.Config.Password = "Pass"

	_, err = goes.ReadStreamEventsBackward(conn, streamID, -1, 2, true, true)
	if err == nil {
		t.Fatalf("Expected failure")
	}
	expectedError := notAuthenticatedError
	if err.Error() != expectedError {
		t.Fatalf("Expected %s got %s", expectedError, notAuthenticatedError)
	}
}
