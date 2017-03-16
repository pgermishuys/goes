package goes

// DecodeNetUUI decodes a dotnet UUID
func DecodeNetUUID(netEncoded []byte) []byte {
	var order = [...]int{3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15}
	uuidBytes := make([]byte, 16)
	for i := 0; i < len(order); i++ {
		uuidBytes[i] = netEncoded[order[i]]
	}
	return uuidBytes
}

// EncodeNetUUID encodes a dotnet UUID
func EncodeNetUUID(uuid []byte) []byte {
	var order = [...]int{3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15}
	uuidBytes := make([]byte, 16)
	for i := 0; i < len(order); i++ {
		uuidBytes[i] = uuid[order[i]]
	}
	return uuidBytes
}
