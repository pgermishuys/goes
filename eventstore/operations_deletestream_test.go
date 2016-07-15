package goes_test

import (
	"testing"

	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

func TestDeleteStream_WithSoftDelete(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	events := []goes.Event{
		createTestEvent(),
	}

	streamID := uuid.NewV4().String()
	result, err := goes.AppendToStream(conn, streamID, -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.Result)
	}
	deleteStreamResult, err := goes.DeleteStream(conn, streamID, 0, false, false)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult = protobuf.OperationResult_Success
	if deleteStreamResult.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, deleteStreamResult.GetResult())
	}

	readResult, err := goes.ReadSingleEvent(conn, streamID, 0, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	readExpectedResult := protobuf.ReadEventCompleted_NoStream
	if readResult.GetResult() != readExpectedResult {
		t.Fatalf("Expected %s got %s", readExpectedResult, readResult.GetResult())
	}
}

func TestDeleteStream_WithHardDelete(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	events := []goes.Event{
		createTestEvent(),
	}

	streamID := uuid.NewV4().String()
	result, err := goes.AppendToStream(conn, streamID, -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.Result)
	}
	deleteStreamResult, err := goes.DeleteStream(conn, streamID, 0, false, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult = protobuf.OperationResult_Success
	if deleteStreamResult.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, deleteStreamResult.GetResult())
	}

	readResult, err := goes.ReadSingleEvent(conn, streamID, 0, true, true)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	readExpectedResult := protobuf.ReadEventCompleted_StreamDeleted
	if readResult.GetResult() != readExpectedResult {
		t.Fatalf("Expected %s got %s", readExpectedResult, readResult.GetResult())
	}
}

func TestDeleteStream_WithWrongExpectedVersion(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()
	events := []goes.Event{
		createTestEvent(),
	}

	streamID := uuid.NewV4().String()
	result, err := goes.AppendToStream(conn, streamID, -2, events)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult := protobuf.OperationResult_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, result.Result)
	}
	deleteStreamResult, err := goes.DeleteStream(conn, streamID, 1, false, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}
	expectedResult = protobuf.OperationResult_WrongExpectedVersion
	if deleteStreamResult.GetResult() != expectedResult {
		t.Fatalf("Expected %s got %s", expectedResult, deleteStreamResult.GetResult())
	}
}

func TestDeleteStream_WithInvalidCredentials(t *testing.T) {
	conn := createTestConnection(t)
	events := []goes.Event{
		createTestEvent(),
	}

	streamID := uuid.NewV4().String()
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
	conn.Config.Login = "Pass"
	_, err = goes.DeleteStream(conn, streamID, 1, false, true)

	if err == nil {
		t.Fatalf("Expected failure")
	}
	expectedError := notAuthenticatedError
	if err.Error() != expectedError {
		t.Fatalf("Expected %s got %s", expectedError, err.Error())
	}
}
