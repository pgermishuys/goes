package goes_test

import (
	"testing"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

func TestReadStreamEventsForward_WithEmptyStream(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	result, err := goes.ReadStreamEventsForward(conn, uuid.NewV4().String(), 0, 1, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.ReadStreamEventsCompleted_NoStream
	if result.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.GetResult())
	}
}

func TestReadStreamEventsForward_WithStreamContainingEvents(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	streamID := uuid.NewV4().String()
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

	readResult, err := goes.ReadStreamEventsForward(conn, streamID, 0, 2, true, true)
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

func TestReadStreamEventsForward_WithReadingMoreEventsThanExistsInStream(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	streamID := uuid.NewV4().String()
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

	readResult, err := goes.ReadStreamEventsForward(conn, streamID, 0, 2, true, true)
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
