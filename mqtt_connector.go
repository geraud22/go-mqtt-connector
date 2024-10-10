package mqtt_connector

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	cfy "github.com/geraud22/config-from-yaml"
)

var Client mqtt.Client
var config = cfy.Get("config")
var handlers = make(map[string]SubscriptionHandler)
var match = func(wildcard, topic string) bool {
	wildcardParts := strings.Split(wildcard, "/")
	topicParts := strings.Split(topic, "/")
	if len(wildcardParts) != len(topicParts) {
		return false
	}
	for i, wildcardPart := range wildcardParts {
		if wildcardPart == "+" {
			continue
		}
		if wildcardPart == topicParts[i] {
			continue
		}
		return false
	}
	return true
}

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	topic := msg.Topic()
	if handler, exists := handlers[msg.Topic()]; exists {
		handler.SendMessageToChannel(msg.Payload())
		return
	}
	for wildcard, handler := range handlers {
		if match(wildcard, topic) {
			handler.SendMessageToChannel(msg.Payload())
			return
		}
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Client Connected\n")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v\n", err)
}

type SubscriptionHandler interface {
	SendMessageToChannel(payload []byte)
	GetChannel() <-chan []byte
	GetErrorChannel() chan error
}

type Handler struct {
	payloadChannel chan []byte
	errorChannel   chan error
}

func (h *Handler) SendMessageToChannel(payload []byte) {
	h.payloadChannel <- payload
}

func (h *Handler) GetChannel() <-chan []byte {
	return h.payloadChannel
}

func (h *Handler) GetErrorChannel() chan error {
	return h.errorChannel
}

func newHandler() *Handler {
	return &Handler{
		payloadChannel: make(chan []byte),
		errorChannel:   make(chan error),
	}
}

// Connect is responsible for connecting the global mqtt.Client.
// It will retrieve the connection information from the client project's config.yml
func Connect() {
	var broker = config.GetString("MQTT.Broker")
	var port = config.GetInt("MQTT.Port")
	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	clientID := config.GetString("MQTT.ClientID")
	username := config.GetString("MQTT.Username")
	password := config.GetString("MQTT.Password")
	opts.SetClientID(clientID)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetDefaultPublishHandler(messagePubHandler)
	opts.SetKeepAlive(60 * time.Second)
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	Client = mqtt.NewClient(opts)
	if token := Client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT: %v", token.Error())
	}
}

// Sub will subscribe to an MQTT topic, only if the client connection has already been established.
//
// Parameters:
// - topicToSub: The string representing the MQTT topic to subscribe to.
//
// Returns:
//   - SubscriptionHandler: An interface which provides a channel where incoming message payloads will be sent,
//     via the package variable messagePubHandler
//   - Error: If the request to subscribe to the given topic times out after 10 seconds, will return error.
func Sub(topicToSub string) (SubscriptionHandler, error) {
	handlers[topicToSub] = newHandler()
	token := Client.Subscribe(topicToSub, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return nil, errors.New("failed to subscribe to topic: " + topicToSub)
	}
	fmt.Printf("Subscribed to topic: %s\n", topicToSub)
	return handlers[topicToSub], nil
}

// AsyncPayloadHandler listens on the channel of the given SubscriptionHandler Interface
// and processes incoming MQTT payloads asynchronously using the provided processFunc.
//
// It continues running until the context is canceled.
// Errors are sent to the handler's error channel.
//
// Parameters:
// - ctx: A context.WithCancel used to control the lifetime of the handler. It should be cancelled to stop the handler gracefully.
// - handler: A SubscriptionHandler that manages the channel through which payloads are received.
// - processFunc: A client-defined function that takes a byte slice (representing the MQTT payload) and processes it.
func AsyncPayloadHandler(ctx context.Context, handler SubscriptionHandler, processFunc func([]byte) error) {
	var wg sync.WaitGroup
	for {
		select {
		case payload := <-handler.GetChannel():
			wg.Add(1)
			go func(payload []byte) {
				defer wg.Done()
				if err := processFunc(payload); err != nil {
					select {
					case handler.GetErrorChannel() <- err:
					case <-ctx.Done():
						return
					}
				}
			}(payload)
		case <-ctx.Done():
			log.Println("payload handler received shutdown signal")
			wg.Wait()
			close(handler.GetErrorChannel())
			return
		}
	}
}

// PayloadHandler listens on the channel of the given SubscriptionHandler Interface
// and processes the incoming MQTT payload using the provided processFunc.
//
// It will only process one payload before exiting.
//
// Parameters:
// - handler: A SubscriptionHandler that manages the channel through which a payload is received.
// - processFunc: A client-defined function that takes a byte slice (representing the MQTT payload) and processes it.
//
// Returns:
// - An error if something goes wrong during processing.
func PayloadHandler(handler SubscriptionHandler, processFunc func([]byte) error) error {
	payload := <-handler.GetChannel()
	if err := processFunc(payload); err != nil {
		return fmt.Errorf("error processing payload: %v", err)
	}
	return nil
}
