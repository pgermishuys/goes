package goes_test

import (
	"testing"

	uuid "github.com/gofrs/uuid"
	"github.com/pgermishuys/goes/eventstore"
	"github.com/pgermishuys/goes/protobuf"
)

func TestCreatePersistentSubscription_CreateNewSubscription(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	settings := goes.NewPersistentSubscriptionSettings()
	groupName := uuid.Must(uuid.NewV4()).String()
	result, err := goes.CreatePersistentSubscription(conn, "testStream", groupName, *settings)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	expectedResult := protobuf.CreatePersistentSubscriptionCompleted_Success
	if *result.Result != expectedResult {
		t.Fatalf("Expected result to be %s, but was %s", expectedResult.String(), result.Result.String())
	}
}

func TestCreatePersistentSubscription_WhenSubscriptionExists(t *testing.T) {
	conn := createTestConnection(t)
	defer conn.Close()

	settings := goes.NewPersistentSubscriptionSettings()
	groupName := uuid.Must(uuid.NewV4()).String()

	_, err := goes.CreatePersistentSubscription(conn, "testStream", groupName, *settings)
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	result, err := goes.CreatePersistentSubscription(conn, "testStream", groupName, *settings)
	if err == nil {
		t.Fatalf("Expected a failure")
	}

	expectedResult := "AlreadyExists"
	if err.Error() != expectedResult {
		t.Fatalf("Expected error to be %s but was %s", expectedResult, err.Error())
	}
	if result.Result.String() != expectedResult {
		t.Fatalf("Expected result to be %s but was %s", expectedResult, result.Result.String())
	}
}
