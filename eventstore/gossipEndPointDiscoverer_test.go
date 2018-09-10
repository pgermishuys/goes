package goes_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	goes "github.com/pgermishuys/goes/eventstore"
)

func TestGossipEndPointDiscoverer_Unavailable(t *testing.T) {
	discoverer := goes.GossipEndpointDiscoverer{
		MaxDiscoverAttempts: 10,
		GossipSeeds:         []string{"127.0.0.1:2113"},
	}
	member, _ := discoverer.Discover()
	if member != (goes.MemberInfo{}) {
		t.Fatalf("Expected Member to be nil, got %+v", member)
	}
}

func TestGossipEndPointDiscoverer_WithAliveMaster(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		gossipResponse := `
        {
			"members": [
				{
				"state": "Manager",
				"isAlive": false,
				"externalTcpIp": "127.0.0.3",
				"externalTcpPort": 3112,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.3",
				"externalHttpPort": 3112
				},
				{
				"state": "Slave",
				"isAlive": true,
				"externalTcpIp": "127.0.0.2",
				"externalTcpPort": 2114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.2",
				"externalHttpPort": 2113
				},
				{
				"state": "Master",
				"isAlive": true,
				"externalTcpIp": "127.0.0.1",
				"externalTcpPort": 1114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.1",
				"externalHttpPort": 1113
				}
			],
			"serverIp": "127.0.0.1",
			"serverPort": 1112
		}`
		fmt.Fprintln(w, gossipResponse)
	}))
	defer server.Close()

	url := server.URL
	discoverer := goes.GossipEndpointDiscoverer{
		MaxDiscoverAttempts: 10,
		GossipSeeds:         []string{url},
	}
	member, err := discoverer.Discover()
	if err != nil {
		t.Fatalf("Discover Failed")
	}
	if member.State != "Master" {
		t.Fatalf("Expected State to be Master but was %s", member.State)
	}
	if member.IsAlive != true {
		t.Fatalf("Expected IsAlive to be true but was %v", member.IsAlive)
	}
}

func TestGossipEndPointDiscoverer_WithTheFirstGossipRequestFailing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		gossipResponse := `
        {
			"members": [
				{
				"state": "Manager",
				"isAlive": false,
				"externalTcpIp": "127.0.0.3",
				"externalTcpPort": 3112,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.3",
				"externalHttpPort": 3112
				},
				{
				"state": "Slave",
				"isAlive": true,
				"externalTcpIp": "127.0.0.2",
				"externalTcpPort": 2114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.2",
				"externalHttpPort": 2113
				},
				{
				"state": "Master",
				"isAlive": true,
				"externalTcpIp": "127.0.0.1",
				"externalTcpPort": 1114,
				"externalSecureTcpPort": 0,
				"externalHttpIp": "127.0.0.1",
				"externalHttpPort": 1113
				}
			],
			"serverIp": "127.0.0.1",
			"serverPort": 1112
		}`
		fmt.Fprintln(w, gossipResponse)
	}))
	defer server.Close()

	url := server.URL
	discoverer := goes.GossipEndpointDiscoverer{
		MaxDiscoverAttempts: 10,
		GossipSeeds:         []string{"127.0.0.1:2113", url},
	}
	member, err := discoverer.Discover()
	if err != nil {
		t.Fatalf("Discover Failed")
	}
	if member.State != "Master" {
		t.Fatalf("Expected State to be Master but was %s", member.State)
	}
	if member.IsAlive != true {
		t.Fatalf("Expected IsAlive to be true but was %v", member.IsAlive)
	}
}
