package eventstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
)

// TCPPackage for describing the TCP Package structure from Event Store
type TCPPackage struct {
	PackageLength uint32
	Command       Command
	Flags         byte
	CorrelationID []byte
	Login         string
	Password      string
	Data          []byte
}

func newPackage(command Command, corrID []byte, login string, password string, data []byte) (TCPPackage, error) {
	var pkg = TCPPackage{
		Command:       command,
		CorrelationID: encodeNetUUID(corrID),
		Flags:         0x00,
	}
	if len(login) > 0 {
		pkg.Flags = 0x01
		pkg.Login = login
		pkg.Password = password
	}
	log.Printf("[info] writing struct into buffer %+v", pkg)
	buf := &bytes.Buffer{}
	err := binary.Write(buf, binary.LittleEndian, pkg)
	if err != nil {
		log.Fatalf("[fatal] failed to write struct to binary %+v", err.Error())
	}
	pkg.PackageLength = uint32(buf.Len())
	//bug here, this ^^ should be 18
	pkg.PackageLength = 18
	return pkg, nil
}

// ParseTCPPackage reads the bytes into a TcpPackage
func parseTCPPackage(bytes []byte) (TCPPackage, error) {
	log.Printf("Received bytes %+v", bytes)
	return TCPPackage{}, nil
}

func (pkg *TCPPackage) write(connection *Connection) error {
	loginBytes := []byte(pkg.Login)
	if len(loginBytes) > 255 {
		return fmt.Errorf("Login is %d bytes, maximum length 255 bytes", len(loginBytes))
	}

	passwordBytes := []byte(pkg.Password)
	if len(passwordBytes) > 255 {
		return fmt.Errorf("Password is %d bytes, maximum length 255 bytes", len(passwordBytes))
	}

	totalMessageLength := minimumTCPPackageSize +
		1 +
		len(loginBytes) +
		1 +
		len(passwordBytes) +
		len(pkg.Data)

	//TODO handle error and nwritten
	_, err := connection.Socket.Write([]byte{
		byte(totalMessageLength),
		byte(totalMessageLength >> 8),
		byte(totalMessageLength >> 16),
		byte(totalMessageLength >> 24),
	})
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write([]byte{
		byte(pkg.Command),
		byte(pkg.Flags),
	})
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write(encodeNetUUID(pkg.CorrelationID))
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write([]byte{byte(len(loginBytes))})
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write(loginBytes)
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write([]byte{byte(len(passwordBytes))})
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write(passwordBytes)
	if err != nil {
		return err
	}
	_, err = connection.Socket.Write(pkg.Data)
	if err != nil {
		return err
	}

	return nil
}

const minimumTCPPackageSize = 0 +
	1 + // Command
	1 + // Flags
	16 //Correlation ID
