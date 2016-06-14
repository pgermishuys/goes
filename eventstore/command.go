package goes

type Command byte

const (
	heartbeatRequest  Command = 0x01
	heartbeatResponse Command = 0x02

	ping Command = 0x03
	pong Command = 0x04

	prepareAck = 0x05
	commitAck  = 0x06

	slaveAssignment = 0x07
	cloneAssignment = 0x08

	subscribeReplica         = 0x10
	replicaLogPositionAck    = 0x11
	createChunk              = 0x12
	rawChunkBulk             = 0x13
	dataChunkBulk            = 0x14
	replicaSubscriptionRetry = 0x15
	replicaSubscribed        = 0x16

	// CLIENT COMMANDS
	//        CreateStream = 0x80
	//        CreateStreamCompleted = 0x81

	writeEvents          = 0x82
	writeEventsCompleted = 0x83

	transactionStart           = 0x84
	transactionStartCompleted  = 0x85
	transactionWrite           = 0x86
	transactionWriteCompleted  = 0x87
	transactionCommit          = 0x88
	transactionCommitCompleted = 0x89

	deleteStream          = 0x8A
	deleteStreamCompleted = 0x8B

	readEvent                         = 0xB0
	readEventCompleted                = 0xB1
	readStreamEventsForward           = 0xB2
	readStreamEventsForwardCompleted  = 0xB3
	readStreamEventsBackward          = 0xB4
	readStreamEventsBackwardCompleted = 0xB5
	readAllEventsForward              = 0xB6
	readAllEventsForwardCompleted     = 0xB7
	readAllEventsBackward             = 0xB8
	readAllEventsBackwardCompleted    = 0xB9

	subscribeToStream                         = 0xC0
	subscriptionConfirmation                  = 0xC1
	streamEventAppeared                       = 0xC2
	unsubscribeFromStream                     = 0xC3
	subscriptionDropped                       = 0xC4
	connectToPersistentSubscription           = 0xC5
	persistentSubscriptionConfirmation        = 0xC6
	persistentSubscriptionStreamEventAppeared = 0xC7
	createPersistentSubscription              = 0xC8
	createPersistentSubscriptionCompleted     = 0xC9
	deletePersistentSubscription              = 0xCA
	deletePersistentSubscriptionCompleted     = 0xCB
	persistentSubscriptionAckEvents           = 0xCC
	persistentSubscriptionNakEvents           = 0xCD
	updatePersistentSubscription              = 0xCE
	updatePersistentSubscriptionCompleted     = 0xCF

	scavengeDatabase          = 0xD0
	scavengeDatabaseCompleted = 0xD1

	badRequest       = 0xF0
	notHandled       = 0xF1
	authenticate     = 0xF2
	authenticated    = 0xF3
	notAuthenticated = 0xF4
)

func (c Command) String() string {
	s := ""
	if c&heartbeatRequest == heartbeatRequest {
		s += "Heartbeat Request"
	}
	if c&heartbeatResponse == heartbeatResponse {
		s += "Heartbeat Response"
	}
	if c&ping == ping {
		s += "Ping"
	}
	if c&pong == pong {
		s += "Pong"
	}
	if c&writeEvents == writeEvents {
		s += "Write Events"
	}
	if c&writeEventsCompleted == writeEventsCompleted {
		s += "Write Events Completed"
	}
	if c&badRequest == badRequest {
		s += "Bad Request"
	}
	if c&notHandled == notHandled {
		s += "Not Handled"
	}
	if c&notAuthenticated == notAuthenticated {
		s += "Not Authenticated"
	}
	return s
}
