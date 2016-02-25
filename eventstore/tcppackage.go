package eventstore

import (
	"encoding/binary"
	"fmt"
	"io"
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

// ParseTCPPackage reads the bytes into a TcpPackage
func parseTCPPackage(reader io.Reader) (TCPPackage, error) {
	var pkg TCPPackage
	err := binary.Read(reader, binary.LittleEndian, &pkg.PackageLength)
	if err != nil {
		return pkg, err
	}
	err = binary.Read(reader, binary.LittleEndian, &pkg.Command)
	if err != nil {
		return pkg, err
	}
	err = binary.Read(reader, binary.LittleEndian, &pkg.Flags)
	if err != nil {
		return pkg, err
	}
	uuid := make([]byte, 16)
	err = binary.Read(reader, binary.LittleEndian, uuid)
	if err != nil {
		return pkg, err
	}
	pkg.CorrelationID = decodeNetUUID(uuid)
	return pkg, nil
}

func (pkg *TCPPackage) write(connection *Connection) error {
	var writer binary.Writer

	loginBytes := []byte(pkg.Login)
	if len(loginBytes) > 255 {
		return fmt.Errorf("Login is %d bytes, maximum length 255 bytes", len(loginBytes))
	}

	passwordBytes := []byte(pkg.Password)
	if len(passwordBytes) > 255 {
		return fmt.Errorf("Password is %d bytes, maximum length 255 bytes", len(passwordBytes))
	}

	totalMessageLength := minimumTcpPackageSize +
		1 +
		len(loginBytes) +
		1 +
		len(passwordBytes) +
		len(pkg.Data)

	//TODO handle error and nwritten
	connection.Socket.Write([]byte{
		byte(totalMessageLength),
		byte(totalMessageLength >> 8),
		byte(totalMessageLength >> 16),
		byte(totalMessageLength >> 24),
	})
	connection.Socket.Write([]byte{
		byte(pkg.Command),
		byte(pkg.Flags),
	})
	connection.Socket.Write(encodeNetUUID(pkg.CorrelationID))
	connection.Socket.Write([]byte{byte(len(loginBytes))})
	connection.Socket.Write(loginBytes)
	connection.Socket.Write([]byte{byte(len(passwordBytes))})
	connection.Socket.Write(passwordBytes)
	connection.Socket.Write(pkg.Data)

	return nil
}

const minimumTcpPackageSize = 0 +
	1 + // Command
	1 + // Flags
	16 //Correlation ID
