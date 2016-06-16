package main

func main() {
	// config := &goes.Configuration{
	// 	Address: "127.0.0.1",
	// 	Port:    1113,
	// }
	// conn, err := goes.NewEventStoreConnection(config)
	// if err != nil {
	// 	log.Fatalf("[fatal] %s", err.Error())
	// }
	// err = conn.Connect()
	// defer conn.Close()
	// if err != nil {
	// 	log.Fatalf("[fatal] %s", err.Error())
	// }
	// events := []goes.Event{
	// 	goes.Event{
	// 		EventID:   uuid.NewV4(),
	// 		EventType: "itemAdded",
	// 		IsJSON:    true,
	// 		Data:      []byte("{\"price\": \"100\"}"),
	// 		Metadata:  []byte("metadata"),
	// 	},
	// 	goes.Event{
	// 		EventID:   uuid.NewV4(),
	// 		EventType: "itemAdded",
	// 		IsJSON:    true,
	// 		Data:      []byte("{\"price\": \"120\"}"),
	// 		Metadata:  []byte("metadata"),
	// 	},
	// }
	// go goes.AppendToStream(conn, "shoppingCart-1", -2, events)
	// go goes.ReadSingleEvent(conn, "$stats-127.0.0.1:2113", 0, true, true)
	// select {}
}
