package eventstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/satori/go.uuid"
	"log"
	"net"
)

type Configuration struct {
	Address string
	Port    int
}
type Connection struct {
	Config *Configuration
	Socket *net.TCPConn
}

// Connect attempts to connect to Event Store using the given configuration
func (connection *Connection) Connect() error {
	log.Print("[info] connecting to event store...")

	address := fmt.Sprintf("%s:%v", connection.Config.Address, connection.Config.Port)
	resolvedAddress, _ := net.ResolveTCPAddr("tcp", address)
	conn, err := net.DialTCP("tcp", nil, resolvedAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to event store on %+v. details: %s", address, err.Error())
	}
	log.Printf("[info] succesfully connected to event store on %s", address)
	connection.Socket = conn
	receiver := make(chan []byte)

	go startRead(connection, receiver)
	return nil
}

// Close attempts to close the connection to Event Store
func (connection *Connection) Close() error {
	log.Print("[info] closing the connection to event store...")
	return connection.Socket.Close()
}

func startRead(connection *Connection, receiver chan []byte) {
	buffer := make([]byte, 1024)
	for {
		fmt.Println("[info] heartbeat")
		written, err := connection.Socket.Read(buffer)
		if err != nil {
			log.Fatal(err.Error())
		}
		msg, err := ParseTCPPackage(bytes.NewReader(buffer))
		if err != nil {
			log.Fatalf("[fatal] could not decode tcp package: %+v\n", err.Error())
		}
		switch msg.Command {
		case HeartbeatRequest:
			log.Printf("[info] received heartbeat request of %+v bytes", written)
			sendCommand(HeartbeatResponse, msg, connection.Socket)
			break
		case Pong:
			log.Printf("[info] received reply for ping of %+v bytes", written)
			sendCommand(Ping, TCPPackage{CorrelationID: uuid.NewV4()}, connection.Socket)
			break
		case 0x0F:
			log.Fatal("[fatal] bad request sent")
			break
		}
	}
}

// NewConnection sets up a new Event Store Connection but does not open the connection
func NewConnection(config *Configuration) (*Connection, error) {
	if len(config.Address) == 0 {
		return nil, fmt.Errorf("The address (%v) cannot be an empty string", config.Address)
	}
	if config.Port <= 0 {
		return nil, fmt.Errorf("The port (%v) cannot be less or equal to 0", config.Port)
	}
	return &Connection{
		Config: config,
	}, nil
}

func sendCommand(command Command, message TCPPackage, conn *net.TCPConn) (int, error) {
	var pkg = &TCPPackage{
		Command:       command,
		CorrelationID: encodeNetUUID(message.CorrelationID),
		Flags:         0x00,
	}
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, pkg)
	if err != nil {
		log.Fatalf("[error] failed to write struct to binary %+v", err.Error())
	}
	pkg.PackageLength = 18
	buf = &bytes.Buffer{}
	err = binary.Write(buf, binary.LittleEndian, pkg)
	if err != nil {
		log.Fatalf("[error] failed to write struct to binary %+v", err.Error())
	}
	log.Printf("[info] sending %+v with correlation id : %+v", command, message.CorrelationID)
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		log.Fatalf("[error] failed to send command %+v", err.Error())
	}

	return 0, nil
}
