package goes

import (
	"github.com/satori/go.uuid"
)

//Event is a structure that is used to help in marshalling events to and from a tcp package
type Event struct {
	EventID   uuid.UUID
	EventType string
	IsJSON    bool
	Data      []byte
	Metadata  []byte
}
