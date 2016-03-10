package eventstore

import (
	"log"

	"github.com/satori/go.uuid"
)

//AppendToStream appends the given events to a stream
func AppendToStream(conn *Connection, streamID string, expectedVersion int, data []byte) error {
	pkg, err := newPackage(writeEvents, uuid.NewV4().Bytes(), "admin", "changeit", data)
	if err != nil {
		log.Printf("[error] failed to create new write events package")
	}
	sendPackage(pkg, conn)
	return nil
}
