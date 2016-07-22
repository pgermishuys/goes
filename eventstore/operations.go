package goes

import (
	"errors"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

const (
	inspectionDecision_EndOperation inspectionDecision = 0
	inspectionDecision_Retry        inspectionDecision = 1
)

type inspectionDecision int32

type inspectResult func(result TCPPackage) (inspectionDecision, error)

type clientOperation struct {
	networkPackage TCPPackage
	inspectResult  inspectResult
	retryCount     int
}

func HandleOperation(conn *EventStoreConnection, op *clientOperation) (TCPPackage, error) {
	resultChan := make(chan TCPPackage)
	sendPackage(op.networkPackage, conn, resultChan)
	result := <-resultChan

	decision, err := op.inspectResult(result)
	if err != nil {
		return result, err
	}

	if decision == inspectionDecision_Retry {
		if op.retryCount < conn.Config.MaxOperationRetries {
			op.retryCount++
			log.Printf("[info] retrying %+v command. Retry attempt %v of %v", op.networkPackage.Command.String(), op.retryCount+1, conn.Config.MaxOperationRetries)
			return HandleOperation(conn, op)
		} else {
			log.Printf("[error] command %v failed. Retry limit of %v reached", op.networkPackage.Command.String(), conn.Config.MaxOperationRetries)
		}
	}
	return result, err
}

func AppendToStream(conn *EventStoreConnection, streamID string, expectedVersion int32, evnts []Event) (protobuf.WriteEventsCompleted, error) {
	var events []*protobuf.NewEvent
	for _, evnt := range evnts {
		dataContentType := int32(0)
		if evnt.IsJSON == true {
			dataContentType = 1
		}
		events = append(events,
			&protobuf.NewEvent{
				EventId:             EncodeNetUUID(evnt.EventID.Bytes()),
				EventType:           proto.String(evnt.EventType),
				DataContentType:     proto.Int32(dataContentType),
				MetadataContentType: proto.Int32(0),
				Data:                evnt.Data,
				Metadata:            evnt.Metadata,
			},
		)
	}
	writeEventsData := &protobuf.WriteEvents{
		EventStreamId:   proto.String(streamID),
		ExpectedVersion: proto.Int32(expectedVersion),
		Events:          events,
		RequireMaster:   proto.Bool(true),
	}

	data, err := proto.Marshal(writeEventsData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	pkg, err := newPackage(writeEvents, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password, data)
	if err != nil {
		log.Printf("[error] failed to create new write events package")
	}

	inspect := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != writeEventsCompleted {
			return inspectionDecision(inspectionDecision_EndOperation), errors.New(result.Command.String())
		}

		message := &protobuf.WriteEventsCompleted{}
		proto.Unmarshal(result.Data, message)

		res := message.Result
		log.Printf("[info] WriteEventsCompleted result: %v\n", res)
		switch *res {
		case protobuf.OperationResult_Success:
			return inspectionDecision(inspectionDecision_EndOperation), nil
		case protobuf.OperationResult_PrepareTimeout, protobuf.OperationResult_CommitTimeout, protobuf.OperationResult_ForwardTimeout:
			return inspectionDecision(inspectionDecision_Retry), nil
		case protobuf.OperationResult_WrongExpectedVersion, protobuf.OperationResult_StreamDeleted, protobuf.OperationResult_InvalidTransaction, protobuf.OperationResult_AccessDenied:
			return inspectionDecision(inspectionDecision_EndOperation), errors.New(res.String())
		default:
			log.Printf("[warning] unknown operation result %v\n", res.String())
			return inspectionDecision(inspectionDecision_EndOperation), nil
		}
	}

	op := clientOperation{
		networkPackage: pkg,
		inspectResult:  inspect,
	}

	result, err := HandleOperation(conn, &op)
	message := &protobuf.WriteEventsCompleted{}
	proto.Unmarshal(result.Data, message)
	return *message, err
}

func ReadSingleEvent(conn *EventStoreConnection, streamID string, eventNumber int32, resolveLinkTos bool, requireMaster bool) (protobuf.ReadEventCompleted, error) {
	readEventsData := &protobuf.ReadEvent{
		EventStreamId:  proto.String(streamID),
		EventNumber:    proto.Int32(eventNumber),
		ResolveLinkTos: proto.Bool(resolveLinkTos),
		RequireMaster:  proto.Bool(requireMaster),
	}
	data, err := proto.Marshal(readEventsData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	pkg, err := newPackage(readEvent, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password, data)
	if err != nil {
		log.Printf("[error] failed to create new read event package")
	}

	inspect := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != readEventCompleted {
			return inspectionDecision_EndOperation, errors.New(result.Command.String())
		}

		complete := &protobuf.ReadEventCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] ReadEventCompleted result: %v\n", res)

		switch *res {
		case protobuf.ReadEventCompleted_Success, protobuf.ReadEventCompleted_NotFound, protobuf.ReadEventCompleted_NoStream, protobuf.ReadEventCompleted_StreamDeleted:
			return inspectionDecision_EndOperation, nil
		case protobuf.ReadEventCompleted_AccessDenied, protobuf.ReadEventCompleted_Error:
			return inspectionDecision_EndOperation, errors.New(res.String())
		default:
			log.Printf("[warning] unknown operation result %v\n", res.String())
			return inspectionDecision_EndOperation, nil
		}
	}

	op := clientOperation{
		networkPackage: pkg,
		inspectResult:  inspect,
	}
	result, err := HandleOperation(conn, &op)

	complete := &protobuf.ReadEventCompleted{}
	proto.Unmarshal(result.Data, complete)
	if err == nil && complete.GetResult() == protobuf.ReadEventCompleted_Success {
		complete.Event.Event.EventId = DecodeNetUUID(complete.Event.Event.EventId)
	}
	return *complete, err
}

func DeleteStream(conn *EventStoreConnection, streamID string, expectedVersion int32, requireMaster bool, hardDelete bool) (protobuf.DeleteStreamCompleted, error) {
	deleteStreamData := &protobuf.DeleteStream{
		EventStreamId:   proto.String(streamID),
		ExpectedVersion: proto.Int32(expectedVersion),
		RequireMaster:   proto.Bool(requireMaster),
		HardDelete:      proto.Bool(hardDelete),
	}
	data, err := proto.Marshal(deleteStreamData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	log.Printf("[info] Deleting Stream: %+v\n", deleteStreamData)
	pkg, err := newPackage(deleteStream, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password, data)
	if err != nil {
		log.Printf("[error] failed to create new delete stream package")
	}

	inspect := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != deleteStreamCompleted {
			return inspectionDecision_EndOperation, errors.New(result.Command.String())
		}

		complete := &protobuf.DeleteStreamCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] DeleteStreamCompleted result: %+v\n", res.String())

		switch *res {
		case protobuf.OperationResult_Success:
			return inspectionDecision_EndOperation, nil
		case protobuf.OperationResult_CommitTimeout, protobuf.OperationResult_PrepareTimeout, protobuf.OperationResult_ForwardTimeout:
			return inspectionDecision_Retry, errors.New(res.String())
		case protobuf.OperationResult_AccessDenied, protobuf.OperationResult_InvalidTransaction, protobuf.OperationResult_StreamDeleted, protobuf.OperationResult_WrongExpectedVersion:
			return inspectionDecision_EndOperation, errors.New(res.String())
		default:
			log.Printf("[warning] unknown operation result %v\n", res.String())
			return inspectionDecision_EndOperation, nil
		}
	}

	op := clientOperation{
		networkPackage: pkg,
		inspectResult:  inspect,
	}
	result, err := HandleOperation(conn, &op)
	complete := &protobuf.DeleteStreamCompleted{}
	proto.Unmarshal(result.Data, complete)
	return *complete, err
}

func ReadStreamEventsForward(conn *EventStoreConnection, streamID string, from int32, maxCount int32, resolveLinkTos bool, requireMaster bool) (protobuf.ReadStreamEventsCompleted, error) {
	readStreamEventsForwardData := &protobuf.ReadStreamEvents{
		EventStreamId:   proto.String(streamID),
		FromEventNumber: proto.Int32(from),
		MaxCount:        proto.Int32(maxCount),
		ResolveLinkTos:  proto.Bool(resolveLinkTos),
		RequireMaster:   proto.Bool(requireMaster),
	}
	data, err := proto.Marshal(readStreamEventsForwardData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	log.Printf("[info] Read Stream Forward: %+v\n", readStreamEventsForwardData)
	pkg, err := newPackage(readStreamEventsForward, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password, data)
	if err != nil {
		log.Println("[error] failed to create new read events forward stream package")
	}

	inspect := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != readStreamEventsForwardCompleted {
			return inspectionDecision_EndOperation, errors.New(result.Command.String())
		}
		complete := &protobuf.ReadStreamEventsCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] ReadStreamEventsForwardCompleted result: %+v\n", res.String())

		switch *res {
		case protobuf.ReadStreamEventsCompleted_Success:
			return inspectionDecision_EndOperation, nil
		case protobuf.ReadStreamEventsCompleted_StreamDeleted, protobuf.ReadStreamEventsCompleted_NoStream:
			return inspectionDecision_EndOperation, nil
		case protobuf.ReadStreamEventsCompleted_AccessDenied, protobuf.ReadStreamEventsCompleted_Error:
			return inspectionDecision_EndOperation, errors.New(res.String())
		default:
			log.Printf("[warning] unknown read stream result %v\n", res.String())
			return inspectionDecision_EndOperation, nil
		}
	}

	op := clientOperation{
		networkPackage: pkg,
		inspectResult:  inspect,
	}

	result, err := HandleOperation(conn, &op)
	complete := &protobuf.ReadStreamEventsCompleted{}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] ReadStreamEventsForwardCompleted: %+v\n", complete)

	if err == nil && complete.GetResult() == protobuf.ReadStreamEventsCompleted_Success {
		for _, evnt := range complete.GetEvents() {
			evnt.Event.EventId = DecodeNetUUID(evnt.Event.EventId)
			if evnt.Link != nil {
				evnt.Link.EventId = DecodeNetUUID(evnt.Link.EventId)
			}
		}
	}
	return *complete, err
}

func ReadStreamEventsBackward(conn *EventStoreConnection, streamID string, from int32, maxCount int32, resolveLinkTos bool, requireMaster bool) (protobuf.ReadStreamEventsCompleted, error) {
	readStreamEventsBackwardData := &protobuf.ReadStreamEvents{
		EventStreamId:   proto.String(streamID),
		FromEventNumber: proto.Int32(from),
		MaxCount:        proto.Int32(maxCount),
		ResolveLinkTos:  proto.Bool(resolveLinkTos),
		RequireMaster:   proto.Bool(requireMaster),
	}
	data, err := proto.Marshal(readStreamEventsBackwardData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	log.Printf("[info] Read Stream Backward: %+v\n", readStreamEventsBackwardData)
	pkg, err := newPackage(readStreamEventsBackward, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password, data)
	if err != nil {
		log.Printf("[error] failed to create new read events backward stream package")
	}

	inspect := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != readStreamEventsBackwardCompleted {
			return inspectionDecision_EndOperation, errors.New(result.Command.String())
		}
		complete := &protobuf.ReadStreamEventsCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] ReadStreamEventsBackwardCompleted result: %+v\n", res.String())

		switch *res {
		case protobuf.ReadStreamEventsCompleted_Success:
			return inspectionDecision_EndOperation, nil
		case protobuf.ReadStreamEventsCompleted_StreamDeleted, protobuf.ReadStreamEventsCompleted_NoStream:
			return inspectionDecision_EndOperation, nil
		case protobuf.ReadStreamEventsCompleted_AccessDenied, protobuf.ReadStreamEventsCompleted_Error:
			return inspectionDecision_EndOperation, errors.New(res.String())
		default:
			log.Printf("[warning] unknown read stream result %v\n", res.String())
			return inspectionDecision_EndOperation, nil
		}
	}

	op := clientOperation{
		networkPackage: pkg,
		inspectResult:  inspect,
	}
	result, err := HandleOperation(conn, &op)

	complete := &protobuf.ReadStreamEventsCompleted{}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] ReadStreamEventsBackwardCompleted: %+v\n", complete)

	if err == nil && complete.GetResult() == protobuf.ReadStreamEventsCompleted_Success {
		for _, evnt := range complete.GetEvents() {
			evnt.Event.EventId = DecodeNetUUID(evnt.Event.EventId)
			if evnt.Link != nil {
				evnt.Link.EventId = DecodeNetUUID(evnt.Link.EventId)
			}
		}
	}
	return *complete, err
}

type eventAppeared func(*protobuf.StreamEventAppeared)
type dropped func(*protobuf.SubscriptionDropped)

func SubscribeToStream(conn *EventStoreConnection, streamID string, resolveLinkTos bool, eventAppeared eventAppeared, dropped dropped) (*Subscription, error) {
	subscriptionData := &protobuf.SubscribeToStream{
		EventStreamId:  proto.String(streamID),
		ResolveLinkTos: proto.Bool(resolveLinkTos),
	}
	data, err := proto.Marshal(subscriptionData)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}

	log.Printf("[info] Subscription Data: %+v\n", subscriptionData)
	correlationID := uuid.NewV4()
	pkg, err := newPackage(subscribeToStream, correlationID.Bytes(), conn.Config.Login, conn.Config.Password, data)
	if err != nil {
		log.Printf("[error] failed to subscribe to stream package")
	}
	if !conn.connected {
		return nil, errors.New("the connection is closed")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	subscriptionConfirmation := &protobuf.SubscriptionConfirmation{}
	proto.Unmarshal(result.Data, subscriptionConfirmation)
	log.Printf("[info] SubscribeToStream: %+v\n", subscriptionConfirmation)
	subscription, err := NewSubscription(conn, correlationID, resultChan, eventAppeared, dropped)
	if err != nil {
		log.Printf("[error] Failed to create new subscription: %+v\n", err)
	}
	conn.subscriptions[correlationID] = subscription
	return subscription, nil
}
