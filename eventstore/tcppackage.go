package eventstore

import (
	"encoding/binary"
	"io"
)

// TCPPackage for describing the TCP Package structure from Event Store
type TCPPackage struct {
	PackageLength uint32
	Command       Command
	Flags         byte
	CorrelationID [16]byte
	Login         [0]byte
	Password      [0]byte
	Data          [0]byte
}

// ParseTCPPackage reads the bytes into a TcpPackage
func ParseTCPPackage(reader io.Reader) (TCPPackage, error) {
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
