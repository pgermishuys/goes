package eventstore

import (
	"log"

	"github.com/satori/go.uuid"
)

//AppendToStream appends the given events to a stream
func AppendToStream(conn *Connection, streamID string, expectedVersion int, data []byte) error {
	eventID, _ := uuid.FromString("2499b281-de4f-43cb-93dc-19912736b4f6")
	pkg, err := newPackage(writeEvents, eventID.Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new write events package")
	}
	sendPackage(pkg, conn)
	return nil
}
