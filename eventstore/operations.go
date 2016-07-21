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
		log.Printf("[error] %s", err.Error())
	}

	if decision == inspectionDecision_Retry {
		if op.retryCount < conn.Config.MaxOperationRetries {
			log.Printf("[info] retrying %+v command. Retry attempt %v of %v", op.networkPackage.Command.String(), op.retryCount+1, conn.Config.MaxOperationRetries)
			// retry
		} else {
			log.Printf("[error] command %v failed. Retry limit of %v reached", op.networkPackage.Command.String(), conn.Config.MaxOperationRetries)
		}
	}
	log.Printf("Decision has been reached for %v, %v. Decision: %v", result.Command.String(), decision)
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
		switch *res {
		case protobuf.OperationResult_Success:
			return inspectionDecision(inspectionDecision_EndOperation), nil
		case protobuf.OperationResult_PrepareTimeout, protobuf.OperationResult_CommitTimeout, protobuf.OperationResult_ForwardTimeout:
			return inspectionDecision(inspectionDecision_Retry), nil
		case protobuf.OperationResult_WrongExpectedVersion, protobuf.OperationResult_StreamDeleted, protobuf.OperationResult_InvalidTransaction, protobuf.OperationResult_AccessDenied:
			return inspectionDecision(inspectionDecision_EndOperation), nil
		default:
			log.Println("[warning] unknown operation result %v", res.String())
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

	// complete := &protobuf.WriteEventsCompleted{}
	// if result.Command != writeEventsCompleted {
	// 	return *complete, errors.New(result.Command.String())
	// }
	// proto.Unmarshal(result.Data, complete)
	// log.Printf("[info] WriteEventsCompleted: %+v\n", complete)
	// return *complete, nil
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
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.ReadEventCompleted{}
	if result.Command != readEventCompleted {
		return *complete, errors.New(result.Command.String())
	}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] ReadEventCompleted: %+v\n", complete)
	if complete.GetResult() == protobuf.ReadEventCompleted_Success {
		complete.Event.Event.EventId = DecodeNetUUID(complete.Event.Event.EventId)
	}
	return *complete, nil
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
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.DeleteStreamCompleted{}
	if result.Command != deleteStreamCompleted {
		return *complete, errors.New(result.Command.String())
	}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] DeleteStreamCompleted: %+v\n", complete)
	return *complete, nil
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
		log.Printf("[error] failed to create new read events forward stream package")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.ReadStreamEventsCompleted{}
	if result.Command != readStreamEventsForwardCompleted {
		return *complete, errors.New(result.Command.String())
	}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] ReadStreamEventsForwardCompleted: %+v\n", complete)
	if complete.GetResult() == protobuf.ReadStreamEventsCompleted_Success {
		for _, evnt := range complete.GetEvents() {
			evnt.Event.EventId = DecodeNetUUID(evnt.Event.EventId)
			if evnt.Link != nil {
				evnt.Link.EventId = DecodeNetUUID(evnt.Link.EventId)
			}
		}
	}
	return *complete, nil
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
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.ReadStreamEventsCompleted{}
	if result.Command != readStreamEventsBackwardCompleted {
		return *complete, errors.New(result.Command.String())
	}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] ReadStreamEventsBackwardCompleted: %+v\n", complete)
	if complete.GetResult() == protobuf.ReadStreamEventsCompleted_Success {
		for _, evnt := range complete.GetEvents() {
			evnt.Event.EventId = DecodeNetUUID(evnt.Event.EventId)
			if evnt.Link != nil {
				evnt.Link.EventId = DecodeNetUUID(evnt.Link.EventId)
			}
		}
	}
	return *complete, nil
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
