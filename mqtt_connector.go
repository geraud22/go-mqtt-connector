package mqtt_connector

import (
	"errors"
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	cfy "github.com/geraud22/config-from-yaml"
)

var config = cfy.Get("config")
var Client mqtt.Client
var handlers = make(map[string]SubscriptionHandler)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	if handler, exists := handlers[msg.Topic()]; !exists {
		handler.SendMessageToChannel(msg.Payload())
	} else {
		log.Printf("No handler registered for topic: %s", msg.Topic())
	}
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Client Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
}

type SubscriptionHandler interface {
	SendMessageToChannel(payload []byte)
	GetChannel() <-chan []byte
}

type Handler struct {
	payloadChannel chan []byte
}

func (h *Handler) SendMessageToChannel(payload []byte) {
	h.payloadChannel <- payload
}

func (h *Handler) GetChannel() <-chan []byte {
	return h.payloadChannel
}

func NewHandler() *Handler {
	return &Handler{
		payloadChannel: make(chan []byte),
	}
}

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

func Sub(topicToSub string) (*Handler, error) {
	handler := NewHandler()
	handlers[topicToSub] = handler
	token := Client.Subscribe(topicToSub, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return nil, errors.New("failed to subscribe to topic: " + topicToSub)
	}
	fmt.Printf("Subscribed to topic: %s", topicToSub)
	return handler, nil
}
