GOES is an Event Store golang TCP Library for [Go](https://golang.org). 

[![Build Status](https://app.wercker.com/status/070a83e021d240488762de7d2fb01193/s/master "wercker status")](https://app.wercker.com/project/bykey/070a83e021d240488762de7d2fb01193)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat)](https://godoc.org/github.com/pgermishuys/goes)
[![Coverage Status](https://coveralls.io/repos/github/pgermishuys/goes/badge.svg?branch=HEAD)](https://coveralls.io/github/pgermishuys/goes?branch=HEAD)
[![Go Report Card](https://goreportcard.com/badge/github.com/pgermishuys/goes)](https://goreportcard.com/report/github.com/pgermishuys/goes)

# Example

## Create a configuration that will be used to describe how to connect to Event Store
```Go
//connect to a single node
config := &goes.Configuration{
    ReconnectionDelay:   10000,
    MaxReconnects:       10,
    MaxOperationRetries: 10,
    Address:             "127.0.0.1",
    Port:                1113,
    Login:               "admin",
    Password:            "changeit",
}

//or a cluster via gossip seeds
config := &goes.Configuration{
    ReconnectionDelay:   10000,
    MaxReconnects:       10,
    MaxOperationRetries: 10,
    Login:               "admin",
    Password:            "changeit",
    EndpointDiscoverer:  discoverer = goes.GossipEndpointDiscoverer{
        MaxDiscoverAttempts: 10,
        GossipSeeds:         []string{"http://127.0.0.1:2113", "http://127.0.0.1:1113"},
    }
}
```

## Connect to Event Store
```Go
conn, err := goes.NewEventStoreConnection(config)
if err != nil {
	log.Fatalf("[fatal] %s", err.Error())
}
err = conn.Connect()
defer conn.Close()
if err != nil {
	log.Fatalf("[fatal] %s", err.Error())
}
```

## Write events to Event Store
```Go
events := []goes.Event{
	goes.Event{
		EventID:   uuid.NewV4(),
		EventType: "itemAdded",
		IsJSON:    true,
		Data:      []byte("{\"price\": \"100\"}"),
		Metadata:  []byte("metadata"),
	},
	goes.Event{
		EventID:   uuid.NewV4(),
		EventType: "itemAdded",
		IsJSON:    true,
		Data:      []byte("{\"price\": \"120\"}"),
		Metadata:  []byte("metadata"),
	},
}

result, err := goes.AppendToStream(conn, "shoppingCart-1", -2, events)
if *result.Result != protobuf.OperationResult_Success {
	log.Printf("[info] WriteEvents failed. %v", result.Result.String())
}
if err != nil {
	log.Printf("[error] WriteEvents failed. %v", err.Error())
}
```

## Reading from Event Store
```Go
goes.ReadSingleEvent(conn, "$stats-127.0.0.1:2113", 0, true, true)
```

# LICENSE
Licenced under [MIT](LICENSE).
