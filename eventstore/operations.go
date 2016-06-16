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
	return protobuf.ReadEventCompleted{}, nil
}
