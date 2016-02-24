package eventstore

type Command byte

const (
	HeartbeatRequest  Command = 0x01
	HeartbeatResponse Command = 0x02
	Ping              Command = 0x03
	Pong              Command = 0x04
)

func (c Command) String() string {
	s := ""
	if c&HeartbeatRequest == HeartbeatRequest {
		s += "Heartbeat Request"
	}
	if c&HeartbeatResponse == HeartbeatResponse {
		s += "Heartbeat Response"
	}
	if c&Ping == Ping {
		s += "Ping"
	}
	if c&Pong == Pong {
		s += "Pong"
	}
	return s
}
