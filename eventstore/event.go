package eventstore

import (
	"github.com/satori/go.uuid"
)

type Event struct {
	EventID   uuid.UUID
	EventType string
	IsJSON    bool
	Data      []byte
	Metadata  []byte
}
