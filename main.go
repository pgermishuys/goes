package main

import (
	"bytes"
	"encoding/binary"
	"github.com/satori/go.uuid"
	"io"
	"log"
	"net"
	"os"
)

func main() {
	addr := "127.0.0.1:1113"
	resolvedAddress, _ := net.ResolveTCPAddr("tcp", addr)
	conn, err := net.DialTCP("tcp", nil, resolvedAddress)
	if err != nil {
		log.Fatalf("[error] Cannot connect to %s error: %s", addr, err.Error())
	}
	log.Printf("[info] succesfully connected to event store on %+v", addr)
	//sendCommand(0x03, Message{CorrelationID: uuid.NewV4()}, conn)
	go forever(conn)
	select {}
}

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

func decodeNetUUID(netEncoded []byte) uuid.UUID {
	var order = [...]int{3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15}
	uuidBytes := make([]byte, 16)
	for i := 0; i < len(order); i++ {
		uuidBytes[i] = netEncoded[order[i]]
	}
	id, _ := uuid.FromBytes(uuidBytes)
	return id
}
func encodeNetUUID(uuid uuid.UUID) [16]byte {
	var order = [...]int{3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15}
	uuidBytes := [16]byte{}
	bytesToConvert := uuid.Bytes()
	for i := 0; i < len(order); i++ {
		uuidBytes[i] = bytesToConvert[order[i]]
	}
	return uuidBytes
}

func parseResponse(buffer io.Reader) (Message, error) {
	msg := Message{}
	err := binary.Read(buffer, binary.LittleEndian, &msg)
	if err != nil {
		return msg, err
	}
	return msg, nil
}

func parseTCPPackage(reader io.Reader) (Message, error) {
	var message Message
	err := binary.Read(reader, binary.LittleEndian, &message.MessageLength)
	if err != nil {
		return message, err
	}
	err = binary.Read(reader, binary.LittleEndian, &message.Command)
	if err != nil {
		return message, err
	}
	err = binary.Read(reader, binary.LittleEndian, &message.Flags)
	if err != nil {
		return message, err
	}
	uuid := make([]byte, 16)
	err = binary.Read(reader, binary.LittleEndian, uuid)
	if err != nil {
		return message, err
	}
	message.CorrelationID = decodeNetUUID(uuid)
	return message, nil
}

func sendCommand(command Command, message Message, conn *net.TCPConn) (int, error) {
	var msg = &Message{
		Command:       command,
		CorrelationID: encodeNetUUID(message.CorrelationID),
		Flags:         0x00,
	}
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, msg)
	if err != nil {
		log.Fatalf("[error] failed to write struct to binary %+v", err.Error())
	}
	msg.MessageLength = 18
	buf = &bytes.Buffer{}
	err = binary.Write(buf, binary.LittleEndian, msg)
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

var sentCommand bool

func forever(conn *net.TCPConn) {
	buffer := make([]byte, 1024)
	for {
		written, err := conn.Read(buffer)
		if err != nil {
			log.Fatal("Boom!")
			os.Exit(0)
		}
		msg, err := parseTCPPackage(bytes.NewReader(buffer))
		if err != nil {
			log.Fatalf("[fatal] could not decode tcp package: %+v\n", err.Error())
		}
		switch msg.Command {
		case HeartbeatRequest:
			log.Printf("[info] received heartbeat request of %+v bytes", written)
			sendCommand(HeartbeatResponse, msg, conn)
			break
		case Pong:
			log.Printf("[info] received reply for ping of %+v bytes", written)
			sendCommand(Ping, Message{CorrelationID: uuid.NewV4()}, conn)
			break
		case 0x0F:
			log.Fatal("[fatal] bad request sent")
			break
		}
	}
}

type Message struct {
	MessageLength uint32
	Command       Command
	Flags         byte
	CorrelationID [16]byte
	Login         [0]byte
	Password      [0]byte
	Data          [0]byte
}

// create connection
// connect
// ping
// get pong
