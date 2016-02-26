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

	go startRead(connection)
	return nil
}

// Close attempts to close the connection to Event Store
func (connection *Connection) Close() error {
	log.Print("[info] closing the connection to event store...")
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
	buffer := make([]byte, 1024)
	for {
		fmt.Println("[info] heartbeat")
		written, err := connection.Socket.Read(buffer)
		if err != nil {
			log.Fatal(err.Error())
		}

		// msg, err := parseTCPPackage(bytes.NewReader(buffer))
		msg, err := parseTCPPackage(buffer)
		if err != nil {
			log.Fatalf("[fatal] could not decode tcp package: %+v\n", err.Error())
		}
		switch msg.Command {
		case heartbeatRequest:
			log.Printf("[info] received heartbeat request of %+v bytes", written)
			pkg, err := newPackage(heartbeatResponse, msg.CorrelationID, "", "", nil)
			if err != nil {
				log.Printf("[error] failed to create new heartbeat response package")
			}
			sendPackage(pkg, connection)
			break
		case pong:
			log.Printf("[info] received reply for ping of %+v bytes", written)
			pkg, err := newPackage(ping, uuid.NewV4().Bytes(), "", "", nil)
			if err != nil {
				log.Printf("[error] failed to create new heartbeat response package")
			}
			sendPackage(pkg, connection)
			break
		case 0x0F:
			log.Fatal("[fatal] bad request sent")
			break
		}
	}
}

func sendPackage(pkg TCPPackage, connection *Connection) error {
	log.Printf("[info] sending %+v with correlation id : %+v", pkg.Command, pkg.CorrelationID)
	err := pkg.write(connection)
	if err != nil {
		return err
	}
	return nil
}
