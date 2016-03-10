package main

import (
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/pgermishuys/goes/eventstore"
	"github.com/satori/go.uuid"
)

func main() {
	config := &eventstore.Configuration{
		Address: "127.0.0.1",
		Port:    1113,
	}
	conn, err := eventstore.NewConnection(config)
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	err = conn.Connect()
	defer conn.Close()
	if err != nil {
		log.Fatalf("[fatal] %s", err.Error())
	}
	events := []*NewEvent{
		&NewEvent{
			EventId:             eventstore.EncodeNetUUID(uuid.NewV4().Bytes()),
			EventType:           proto.String("eventtype"),
			DataContentType:     proto.Int32(1),
			MetadataContentType: proto.Int32(0),
			Data:                []byte("data"),
			Metadata:            []byte("metadata"),
		},
	}
	writeEvents := &WriteEvents{
		EventStreamId:   proto.String("streamid"),
		ExpectedVersion: proto.Int32(-2),
		Events:          events,
		RequireMaster:   proto.Bool(true),
	}
	data, err := proto.Marshal(writeEvents)
	if err != nil {
		log.Fatal("marshaling error: ", err)
	}
	log.Printf("[info] protobuf data is %+v bytes", len(data))
	eventstore.AppendToStream(conn, "stream", -1, data)
	select {}
}
