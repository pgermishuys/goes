package goes

import (
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

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

	pkg, err := newPackage(writeEvents, uuid.NewV4().Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new write events package")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.WriteEventsCompleted{}
	proto.Unmarshal(result.Data, complete)
	log.Printf("[info] WriteEventsCompleted: %+v\n", complete)
	return *complete, nil
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

	pkg, err := newPackage(readEvent, uuid.NewV4().Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new read event package")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.ReadEventCompleted{}
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
	pkg, err := newPackage(deleteStream, uuid.NewV4().Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new delete stream package")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.DeleteStreamCompleted{}
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
	pkg, err := newPackage(readStreamEventsForward, uuid.NewV4().Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new read events forward stream package")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.ReadStreamEventsCompleted{}
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
	pkg, err := newPackage(readStreamEventsBackward, uuid.NewV4().Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new read events backward stream package")
	}
	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	complete := &protobuf.ReadStreamEventsCompleted{}
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
	pkg, err := newPackage(subscribeToStream, correlationID.Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to subscribe to stream package")
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
