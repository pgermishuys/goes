package eventstore

type Command byte

const (
	heartbeatRequest  Command = 0x01
	heartbeatResponse Command = 0x02
	ping              Command = 0x03
	pong              Command = 0x04
)

func (c Command) String() string {
	s := ""
	if c&heartbeatRequest == heartbeatRequest {
		s += "Heartbeat Request"
	}
	if c&heartbeatResponse == heartbeatResponse {
		s += "Heartbeat Response"
	}
	if c&ping == ping {
		s += "Ping"
	}
	if c&pong == pong {
		s += "Pong"
	}
	return s
}
