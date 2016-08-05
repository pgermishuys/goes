package goes

import (
	"errors"
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

func marshalToProtobufEvents(evnts []Event) []*protobuf.NewEvent {
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
	return events
}

func performOperation(conn *EventStoreConnection, pkg TCPPackage, expectedResult Command) (TCPPackage, error) {
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	if result.Command != expectedResult {
		return result, errors.New(result.Command.String())
	}
	return result, nil
}

func shouldRetryOperation(operationResult *protobuf.OperationResult) (bool, error) {
	if *operationResult == protobuf.OperationResult_AccessDenied ||
		*operationResult == protobuf.OperationResult_WrongExpectedVersion {
		return false, errors.New(operationResult.String())
	}
	if *operationResult == protobuf.OperationResult_CommitTimeout ||
		*operationResult == protobuf.OperationResult_PrepareTimeout ||
		*operationResult == protobuf.OperationResult_ForwardTimeout {
		return true, nil
	}
	return false, nil
}

func AppendToStream(conn *EventStoreConnection, streamID string, expectedVersion int32, evnts []Event) (protobuf.WriteEventsCompleted, error) {
	events := marshalToProtobufEvents(evnts)
	writeEventsData := &protobuf.WriteEvents{
		EventStreamId:   proto.String(streamID),
		ExpectedVersion: proto.Int32(expectedVersion),
		Events:          events,
		RequireMaster:   proto.Bool(true),
	}

	data, err := proto.Marshal(writeEventsData)
	if err != nil {
		log.Printf("[error] marshaling error: %s", err)
		return protobuf.WriteEventsCompleted{}, err
	}

	pkg, err := newPackage(writeEvents, data, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create new write events package")
		return protobuf.WriteEventsCompleted{}, err
	}

	for i := 0; i < conn.Config.MaxOperationRetries; i++ {
		resultPackage, err := performOperation(conn, pkg, writeEventsCompleted)
		if err != nil {
			return protobuf.WriteEventsCompleted{}, err
		}
		message := &protobuf.WriteEventsCompleted{}
		proto.Unmarshal(resultPackage.Data, message)

		shouldRetry, err := shouldRetryOperation(message.Result)
		if err != nil || !shouldRetry {
			return *message, err
		}
	}

	return protobuf.WriteEventsCompleted{}, errors.New("Retry limit reached")
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

	pkg, err := newPackage(readEvent, data, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create new read event package")
	}

	inspectResult := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != readEventCompleted {
			return EndOperation, errors.New(result.Command.String())
		}
		complete := &protobuf.ReadEventCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] ReadEventCompleted result: %v\n", res)

		return inspectReadEventResult(*res)
	}

	operation := clientOperation{
		tcpPackage:    pkg,
		inspectResult: inspectResult,
	}
	result, err := handleOperation(conn, &operation)

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
	pkg, err := newPackage(deleteStream, data, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create new delete stream package")
	}

	inspectResult := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != deleteStreamCompleted {
			return EndOperation, errors.New(result.Command.String())
		}
		complete := &protobuf.DeleteStreamCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] DeleteStreamCompleted result: %+v\n", res.String())

		return inspectOperationResult(*res)
	}

	operation := clientOperation{
		tcpPackage:    pkg,
		inspectResult: inspectResult,
	}
	result, err := handleOperation(conn, &operation)
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
	pkg, err := newPackage(readStreamEventsForward, data, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Println("[error] failed to create new read events forward stream package")
	}

	inspectResult := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != readStreamEventsForwardCompleted {
			return EndOperation, errors.New(result.Command.String())
		}
		complete := &protobuf.ReadStreamEventsCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] ReadStreamEventsForwardCompleted result: %+v\n", res.String())

		return inspectReadStreamResult(*res)
	}

	operation := clientOperation{
		tcpPackage:    pkg,
		inspectResult: inspectResult,
	}

	result, err := handleOperation(conn, &operation)
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
	pkg, err := newPackage(readStreamEventsBackward, data, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create new read events backward stream package")
	}

	inspectResult := func(result TCPPackage) (inspectionDecision, error) {
		if result.Command != readStreamEventsBackwardCompleted {
			return EndOperation, errors.New(result.Command.String())
		}
		complete := &protobuf.ReadStreamEventsCompleted{}
		proto.Unmarshal(result.Data, complete)
		res := complete.Result
		log.Printf("[info] ReadStreamEventsBackwardCompleted result: %+v\n", res.String())

		return inspectReadStreamResult(*res)
	}

	operation := clientOperation{
		tcpPackage:    pkg,
		inspectResult: inspectResult,
	}
	result, err := handleOperation(conn, &operation)

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
	pkg, err := newPackage(subscribeToStream, data, correlationID.Bytes(), conn.Config.Login, conn.Config.Password)
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
