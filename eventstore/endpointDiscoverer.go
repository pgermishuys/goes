package goes

//EndpointDiscoverer func that is used to discover an endpoint given the gossip seeds
type EndpointDiscoverer interface {
	Discover() (MemberInfo, error)
}
