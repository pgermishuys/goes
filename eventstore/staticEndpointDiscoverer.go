package goes

// StaticEndpointDiscoverer is used for connecting to a single node
type StaticEndpointDiscoverer struct {
	IPAddress string
	Port      int
}

// Discover will just use the given ip address and port to connect to a single node
func (discoverer *StaticEndpointDiscoverer) Discover() (MemberInfo, error) {
	return MemberInfo{
		ExternalTCPIP:   discoverer.IPAddress,
		ExternalTCPPort: discoverer.Port,
	}, nil
}
