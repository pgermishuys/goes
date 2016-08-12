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

	resultPackage, err := performOperation(conn, pkg, readEventCompleted)
	if err != nil {
		return protobuf.ReadEventCompleted{}, err
	}
	message := &protobuf.ReadEventCompleted{}
	proto.Unmarshal(resultPackage.Data, message)

	if *message.Result == protobuf.ReadEventCompleted_AccessDenied ||
		*message.Result == protobuf.ReadEventCompleted_Error {
		return *message, errors.New(message.Result.String())
	}

	if *message.Result == protobuf.ReadEventCompleted_Success {
		message.Event.Event.EventId = DecodeNetUUID(message.Event.Event.EventId)
	}

	return *message, nil
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

	for i := 0; i < conn.Config.MaxOperationRetries; i++ {
		resultPackage, err := performOperation(conn, pkg, deleteStreamCompleted)
		if err != nil {
			return protobuf.DeleteStreamCompleted{}, err
		}
		message := &protobuf.DeleteStreamCompleted{}
		proto.Unmarshal(resultPackage.Data, message)

		shouldRetry, err := shouldRetryOperation(message.Result)
		if err != nil || !shouldRetry {
			return *message, err
		}
	}

	return protobuf.DeleteStreamCompleted{}, errors.New("Retry limit reached")
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

	resultPackage, err := performOperation(conn, pkg, readStreamEventsForwardCompleted)
	if err != nil {
		return protobuf.ReadStreamEventsCompleted{}, err
	}
	message := &protobuf.ReadStreamEventsCompleted{}
	proto.Unmarshal(resultPackage.Data, message)

	if *message.Result == protobuf.ReadStreamEventsCompleted_AccessDenied ||
		*message.Result == protobuf.ReadStreamEventsCompleted_Error {
		return *message, errors.New(message.Result.String())
	}

	if *message.Result == protobuf.ReadStreamEventsCompleted_Success {
		for _, evnt := range message.GetEvents() {
			evnt.Event.EventId = DecodeNetUUID(evnt.Event.EventId)
			if evnt.Link != nil {
				evnt.Link.EventId = DecodeNetUUID(evnt.Link.EventId)
			}
		}
	}

	return *message, nil
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

	resultPackage, err := performOperation(conn, pkg, readStreamEventsBackwardCompleted)
	if err != nil {
		return protobuf.ReadStreamEventsCompleted{}, err
	}
	message := &protobuf.ReadStreamEventsCompleted{}
	proto.Unmarshal(resultPackage.Data, message)

	if *message.Result == protobuf.ReadStreamEventsCompleted_AccessDenied ||
		*message.Result == protobuf.ReadStreamEventsCompleted_Error {
		return *message, errors.New(message.Result.String())
	}

	if *message.Result == protobuf.ReadStreamEventsCompleted_Success {
		for _, evnt := range message.GetEvents() {
			evnt.Event.EventId = DecodeNetUUID(evnt.Event.EventId)
			if evnt.Link != nil {
				evnt.Link.EventId = DecodeNetUUID(evnt.Link.EventId)
			}
		}
	}

	return *message, nil
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

type PersistentSubscriptionSettings struct {
	ResolveLinkTos             bool
	StartFrom                  int
	MessageTimeoutMilliseconds int
	RecordStatistics           bool
	LiveBufferSize             int
	ReadBatchSize              int
	BufferSize                 int
	MaxRetryCount              int
	PreferRoundRobit           bool
	CheckpointAfterTime        int
	CheckpointMaxCount         int
	CheckpointMinCount         int
	SubscriberMaxCount         int
	NamedConsumerStrategy      string
}

func NewPersistentSubscriptionSettings() *PersistentSubscriptionSettings {
	return &PersistentSubscriptionSettings{
		ResolveLinkTos:             false,
		StartFrom:                  -1,
		RecordStatistics:           false,
		MessageTimeoutMilliseconds: 30000,
		BufferSize:                 500,
		LiveBufferSize:             500,
		MaxRetryCount:              10,
		ReadBatchSize:              20,
		CheckpointAfterTime:        2000,
		CheckpointMinCount:         10,
		CheckpointMaxCount:         1000,
		SubscriberMaxCount:         0,
		NamedConsumerStrategy:      "RoundRobin",
	}
}

func CreatePersistentSubscription(conn *EventStoreConnection, streamID string, groupName string, settings PersistentSubscriptionSettings) (protobuf.CreatePersistentSubscriptionCompleted, error) {
	subscriptionData := &protobuf.CreatePersistentSubscription{
		SubscriptionGroupName:      proto.String(groupName),
		EventStreamId:              proto.String(streamID),
		ResolveLinkTos:             proto.Bool(settings.ResolveLinkTos),
		StartFrom:                  proto.Int(settings.StartFrom),
		MessageTimeoutMilliseconds: proto.Int(settings.MessageTimeoutMilliseconds),
		RecordStatistics:           proto.Bool(settings.RecordStatistics),
		LiveBufferSize:             proto.Int(settings.LiveBufferSize),
		ReadBatchSize:              proto.Int(settings.ReadBatchSize),
		BufferSize:                 proto.Int(settings.BufferSize),
		MaxRetryCount:              proto.Int(settings.MaxRetryCount),
		PreferRoundRobin:           proto.Bool(settings.PreferRoundRobit),
		CheckpointAfterTime:        proto.Int(settings.CheckpointAfterTime),
		CheckpointMaxCount:         proto.Int(settings.CheckpointMaxCount),
		CheckpointMinCount:         proto.Int(settings.CheckpointMinCount),
		SubscriberMaxCount:         proto.Int(settings.SubscriberMaxCount),
		NamedConsumerStrategy:      proto.String(settings.NamedConsumerStrategy),
	}

	data, err := proto.Marshal(subscriptionData)
	if err != nil {
		log.Printf("[error] marshaling error: %s", err)
		return protobuf.CreatePersistentSubscriptionCompleted{}, err
	}

	pkg, err := newPackage(createPersistentSubscription, data, uuid.NewV4().Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create new create persistent subscription package")
		return protobuf.CreatePersistentSubscriptionCompleted{}, err
	}

	resultPackage, err := performOperation(conn, pkg, createPersistentSubscriptionCompleted)
	if err != nil {
		return protobuf.CreatePersistentSubscriptionCompleted{}, err
	}
	message := &protobuf.CreatePersistentSubscriptionCompleted{}
	proto.Unmarshal(resultPackage.Data, message)

	if *message.Result == protobuf.CreatePersistentSubscriptionCompleted_AccessDenied ||
		*message.Result == protobuf.CreatePersistentSubscriptionCompleted_Fail ||
		*message.Result == protobuf.CreatePersistentSubscriptionCompleted_AlreadyExists {
		return *message, errors.New(message.Result.String())
	}

	return *message, nil
}

func ConnectToPersistentSubscription(conn *EventStoreConnection, stream string, groupName string, eventAppeared eventAppeared, dropped dropped, bufferSize int, autoAck bool) (*Subscription, error) {
	subscriptionData := &protobuf.ConnectToPersistentSubscription{
		SubscriptionId:          proto.String(groupName),
		EventStreamId:           proto.String(stream),
		AllowedInFlightMessages: proto.Int(bufferSize),
	}

	data, err := proto.Marshal(subscriptionData)
	if err != nil {
		log.Printf("[error] marshalling error: %s", err)
		return nil, err
	}

	correlationID := uuid.NewV4()
	pkg, err := newPackage(connectToPersistentSubscription, data, correlationID.Bytes(), conn.Config.Login, conn.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create new connect to persistent subscription package")
		return nil, err
	}

	if !conn.connected {
		return nil, errors.New("the connection is closed")
	}

	resultChan := make(chan TCPPackage)
	sendPackage(pkg, conn, resultChan)
	result := <-resultChan
	subscriptionConfirmation := &protobuf.PersistentSubscriptionConfirmation{}
	proto.Unmarshal(result.Data, subscriptionConfirmation)
	log.Printf("[info] ConnectToPersistentSubscription: %+v\n", subscriptionConfirmation)
	subscription, err := NewSubscription(conn, correlationID, resultChan, eventAppeared, dropped)
	if err != nil {
		log.Printf("[error] failed to connect to persistent subscription %v\n", err)
		return nil, err
	}
	return subscription, nil
}
