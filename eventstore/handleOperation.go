package goes

import (
	"errors"
	"log"

	"github.com/pgermishuys/goes/protobuf"
)

type inspectionDecision int32

const (
	EndOperation inspectionDecision = 0
	Retry        inspectionDecision = 1
)

type inspectResult func(result TCPPackage) (inspectionDecision, error)

type clientOperation struct {
	tcpPackage    TCPPackage
	inspectResult inspectResult
	retryCount    int
}

func handleOperation(conn *EventStoreConnection, operation *clientOperation) (TCPPackage, error) {
	resultChan := make(chan TCPPackage)
	sendPackage(operation.tcpPackage, conn, resultChan)
	result := <-resultChan

	decision, err := operation.inspectResult(result)
	if err != nil {
		return result, err
	}

	if decision == Retry {
		if operation.retryCount < conn.Config.MaxOperationRetries {
			operation.retryCount++
			log.Printf("[info] retrying %+v command. Retry attempt %v of %v", operation.tcpPackage.Command.String(), operation.retryCount+1, conn.Config.MaxOperationRetries)
			return handleOperation(conn, operation)
		}
		log.Printf("[error] command %v failed. Retry limit of %v reached", operation.tcpPackage.Command.String(), conn.Config.MaxOperationRetries)
	}
	return result, err
}

func inspectOperationResult(res protobuf.OperationResult) (inspectionDecision, error) {
	switch res {
	case protobuf.OperationResult_Success:
		return EndOperation, nil
	case protobuf.OperationResult_PrepareTimeout, protobuf.OperationResult_CommitTimeout, protobuf.OperationResult_ForwardTimeout:
		return Retry, nil
	case protobuf.OperationResult_WrongExpectedVersion, protobuf.OperationResult_StreamDeleted, protobuf.OperationResult_InvalidTransaction, protobuf.OperationResult_AccessDenied:
		return EndOperation, errors.New(res.String())
	default:
		log.Printf("[warning] unknown operation result %v\n", res.String())
		return EndOperation, nil
	}
}

func inspectReadEventResult(res protobuf.ReadEventCompleted_ReadEventResult) (inspectionDecision, error) {
	switch res {
	case protobuf.ReadEventCompleted_Success, protobuf.ReadEventCompleted_NotFound, protobuf.ReadEventCompleted_NoStream, protobuf.ReadEventCompleted_StreamDeleted:
		return EndOperation, nil
	case protobuf.ReadEventCompleted_AccessDenied, protobuf.ReadEventCompleted_Error:
		return EndOperation, errors.New(res.String())
	default:
		log.Printf("[warning] unknown operation result %v\n", res.String())
		return EndOperation, nil
	}
}

func inspectReadStreamResult(res protobuf.ReadStreamEventsCompleted_ReadStreamResult) (inspectionDecision, error) {
	switch res {
	case protobuf.ReadStreamEventsCompleted_Success:
		return EndOperation, nil
	case protobuf.ReadStreamEventsCompleted_StreamDeleted, protobuf.ReadStreamEventsCompleted_NoStream:
		return EndOperation, nil
	case protobuf.ReadStreamEventsCompleted_AccessDenied, protobuf.ReadStreamEventsCompleted_Error:
		return EndOperation, errors.New(res.String())
	default:
		log.Printf("[warning] unknown read stream result %v\n", res.String())
		return EndOperation, nil
	}
}
