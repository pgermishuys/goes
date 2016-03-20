package eventstore

import (
	"fmt"
	"log"
	"net"

	"github.com/satori/go.uuid"
)

type Configuration struct {
	Address string
	Port    int
}

type Connection struct {
	Config   *Configuration
	Socket   *net.TCPConn
	requests map[uuid.UUID]chan<- TCPPackage
}

// Connect attempts to connect to Event Store using the given configuration
func (connection *Connection) Connect() error {
	connection.requests = make(map[uuid.UUID]chan<- TCPPackage)
	log.Print("[info] connecting to event store...\n")

	address := fmt.Sprintf("%s:%v", connection.Config.Address, connection.Config.Port)
	resolvedAddress, _ := net.ResolveTCPAddr("tcp", address)
	conn, err := net.DialTCP("tcp", nil, resolvedAddress)
	if err != nil {
		return fmt.Errorf("failed to connect to event store on %+v. details: %s\n", address, err.Error())
	}
	log.Printf("[info] succesfully connected to event store on %s\n", address)
	connection.Socket = conn

	go startRead(connection)
	return nil
}

// Close attempts to close the connection to Event Store
func (connection *Connection) Close() error {
	log.Print("[info] closing the connection to event store...\n'")
	return connection.Socket.Close()
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

func startRead(connection *Connection) {
	buffer := make([]byte, 40000)
	for {
		_, err := connection.Socket.Read(buffer)
		fmt.Printf("[info] received a package of length: %+v\n", buffer[0])
		if err != nil {
			log.Fatal(err.Error())
		}

		msg, err := parsePackage(buffer)
		if err != nil {
			log.Fatalf("[fatal] could not decode tcp package: %+v\n", err.Error())
		}
		switch msg.Command {
		case heartbeatRequest:
			// log.Printf("[info] received heartbeat request of %+v bytes\n", written)
			pkg, err := newPackage(heartbeatResponse, msg.CorrelationID, "", "", nil)
			if err != nil {
				log.Printf("[error] failed to create new heartbeat response package\n")
			}
			channel := make(chan<- TCPPackage)
			go sendPackage(pkg, connection, channel)
			break
		case pong:
			pkg, err := newPackage(ping, uuid.NewV4().Bytes(), "", "", nil)
			if err != nil {
				log.Printf("[error] failed to create new ping response package")
			}
			channel := make(chan<- TCPPackage)
			go sendPackage(pkg, connection, channel)
			break
		case writeEventsCompleted:
			correlationID, _ := uuid.FromBytes(msg.CorrelationID)
			connection.requests[correlationID] <- msg
			break
		case readEventCompleted:
			correlationID, _ := uuid.FromBytes(msg.CorrelationID)
			connection.requests[correlationID] <- msg
			break
		case 0x0F:
			log.Fatal("[fatal] bad request sent")
			break
		}
	}
}

func sendPackage(pkg TCPPackage, connection *Connection, channel chan<- TCPPackage) error {
	correlationID, _ := uuid.FromBytes(pkg.CorrelationID)
	connection.requests[correlationID] = channel
	err := pkg.write(connection)
	if err != nil {
		return err
	}
	return nil
}
