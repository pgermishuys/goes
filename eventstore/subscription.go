package goes

import (
	"log"

	"github.com/golang/protobuf/proto"
	"github.com/pgermishuys/goes/protobuf"
	"github.com/satori/go.uuid"
)

type Subscription struct {
	CorrelationID uuid.UUID
	Connection    *EventStoreConnection
	Channel       chan TCPPackage
	EventAppeared eventAppeared
	Dropped       dropped
	Started       bool
}

type PersistentSubscription struct {
	CorrelationID uuid.UUID
	Connection    *EventStoreConnection
	Channel       chan TCPPackage
	EventAppeared persistentSubscriptionEventAppeared
	Dropped       dropped
	Started       bool
	AutoAck       bool
}

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

func (subscription *Subscription) Stop() error {
	log.Printf("[info] Stopping subscription")
	subscription.Started = false
	subscription.Connection.requests[subscription.CorrelationID] = nil
	close(subscription.Channel)
	return nil
}
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

func NewPersistentSubscription(connection *EventStoreConnection, correlationID uuid.UUID, channel chan TCPPackage, appeared persistentSubscriptionEventAppeared, dropped dropped, autoAck bool) (*PersistentSubscription, error) {
	subscription := &PersistentSubscription{
		Connection:    connection,
		CorrelationID: correlationID,
		Channel:       channel,
		EventAppeared: appeared,
		Dropped:       dropped,
		AutoAck:       autoAck,
	}
	log.Printf("[info] subscription correlationID %s", correlationID)
	go subscription.Start()
	return subscription, nil
}

func (subscription *PersistentSubscription) Stop() error {
	log.Printf("[info] Stopping persistent subscription")
	subscription.Started = false
	subscription.Connection.requests[subscription.CorrelationID] = nil
	close(subscription.Channel)
	return nil
}

func (subscription *PersistentSubscription) Start() error {
	subscription.Started = true
	for subscription.Started {
		result := <-subscription.Channel
		switch result.Command {
		case persistentSubscriptionStreamEventAppeared:
			eventAppeared := &protobuf.PersistentSubscriptionStreamEventAppeared{}
			err := proto.Unmarshal(result.Data, eventAppeared)
			if err != nil {
			}
			subscription.EventAppeared(eventAppeared)
			if subscription.AutoAck {
				eventIds := [][]byte{
					eventAppeared.Event.GetEvent().EventId,
				}
				subscription.Acknowledge(eventIds)
			}
		case subscriptionDropped:
			subscriptionDropped := &protobuf.SubscriptionDropped{}
			err := proto.Unmarshal(result.Data, subscriptionDropped)
			if err != nil {
			}
			subscription.Dropped(subscriptionDropped)
		}
	}
	return nil
}

func (subscription *PersistentSubscription) Acknowledge(processedEvents [][]byte) error {
	ackData := &protobuf.PersistentSubscriptionAckEvents{
		SubscriptionId:    proto.String(subscription.CorrelationID.String()),
		ProcessedEventIds: processedEvents,
	}

	data, err := proto.Marshal(ackData)
	if err != nil {
		log.Printf("[error] marshalling error: %+v\n", err)
		return err
	}
	pkg, err := newPackage(persistentSubscriptionAckEvents, data, subscription.CorrelationID.Bytes(), subscription.Connection.Config.Login, subscription.Connection.Config.Password)
	if err != nil {
		log.Printf("[error] failed to create ack events package. %+v\n", err)
		return err
	}
	log.Println("[info] acknowleding events")
	err = sendPackage(pkg, subscription.Connection, nil)
	if err != nil {
		log.Printf("[error] failed to send ack events package. %+v\n", err)
		return err
	}

	return nil
}
