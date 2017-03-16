package goes

import (
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

//Subscription represents an Event Store Client Subscription to a stream
type Subscription struct {
	CorrelationID uuid.UUID
	Connection    *EventStoreConnection
	Channel       chan TCPPackage
	EventAppeared eventAppeared
	Dropped       dropped
	Started       bool
}

//NewSubscription creates a new subscription to a stream
func NewSubscription(connection *EventStoreConnection, correlationID uuid.UUID, channel chan TCPPackage, appeared eventAppeared, dropped dropped) (*Subscription, error) {
	subscription := &Subscription{
		Connection:    connection,
		CorrelationID: correlationID,
		Channel:       channel,
		EventAppeared: appeared,
		Dropped:       dropped,
	}
	go subscription.Start()
	return subscription, nil
}

//Stop stops a subscription from receiving events
func (subscription *Subscription) Stop() error {
	log.Printf("[info] Stopping subscription")
	subscription.Started = false
	subscription.Connection.requests[subscription.CorrelationID] = nil
	close(subscription.Channel)
	return nil
}

//Start starts a subscription
func (subscription *Subscription) Start() error {
	subscription.Started = true
	for subscription.Started {
		result := <-subscription.Channel
		switch result.Command {
		case streamEventAppeared:
			eventAppeared := &protobuf.StreamEventAppeared{}
			err := proto.Unmarshal(result.Data, eventAppeared)
			if err != nil {
			}
			subscription.EventAppeared(eventAppeared)
		case subscriptionDropped:
			subscriptionDropped := &protobuf.SubscriptionDropped{}
			err := proto.Unmarshal(result.Data, subscriptionDropped)
			if err != nil {
			}
			subscription.Dropped(subscriptionDropped)
		default:
			//do something meaningful
		}
	}
	return nil
}
