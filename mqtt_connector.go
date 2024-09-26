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
var MqttClient mqtt.Client
var PayloadChannel = make(chan []byte)

var messagePubHandler mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("\nReceived message")
	PayloadChannel <- msg.Payload()
}

var connectHandler mqtt.OnConnectHandler = func(client mqtt.Client) {
	fmt.Println("Client Connected")
}

var connectLostHandler mqtt.ConnectionLostHandler = func(client mqtt.Client, err error) {
	fmt.Printf("Connection lost: %v", err)
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
	opts.OnConnect = connectHandler
	opts.OnConnectionLost = connectLostHandler
	MqttClient = mqtt.NewClient(opts)
	if token := MqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatalf("Error connecting to MQTT: %v", token.Error())
	}
}

func Sub(topicToSub string) error {
	token := MqttClient.Subscribe(topicToSub, 1, nil)
	if ok := token.WaitTimeout(10 * time.Second); !ok {
		return errors.New("failed to subscribe to topic: " + topicToSub)
	}
	fmt.Printf("Subscribed to topic: %s", topicToSub)
	return nil
}
