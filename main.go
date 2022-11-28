package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	broker = "10.29.72.12"
	port   = 1883
	topic  = "1"
	qos    = 1

	WRITETOLOG  = true  // If true then received messages will be written to the console
	WRITETODISK = false // If true then received messages will be written to the file below

	OUTPUTFILE = "/log/receivedMessages.txt"
)

type handler struct {
	f *os.File
}

func NewHandler() *handler {
	var f *os.File
	if WRITETODISK {
		var err error
		f, err = os.Create(OUTPUTFILE)
		if err != nil {
			panic(err)
		}
	}
	return &handler{f: f}
}

// Close closes the file
func (o *handler) Close() {
	if o.f != nil {
		if err := o.f.Close(); err != nil {
			fmt.Printf("ERROR closing file: %s", err)
		}
		o.f = nil
	}
}

// Message
type Message struct {
	Count uint64
}

// handle is called when a message is received
func (o *handler) handle(_ mqtt.Client, msg mqtt.Message) {
	// We extract the count and write that out first to simplify checking for missing values
	//var m Message
	/*if err := json.Unmarshal(msg.Payload(), &m); err != nil {
		fmt.Printf("Message could not be parsed (%s): %s", msg.Payload(), err)
	}*/
	/*if o.f != nil {
		// Write out the number (make it long enough that sorting works) and the payload
		if _, err := o.f.WriteString(fmt.Sprintf("%09d %s\n", m.Count, msg.Payload())); err != nil {
			fmt.Printf("ERROR writing to file: %s", err)
		}
	}*/

	if WRITETOLOG {
		fmt.Printf("received message: %s\n", msg.Payload())
	}
}
func main() {

	h := NewHandler()
	defer h.Close()

	opts := mqtt.NewClientOptions()
	opts.AddBroker(fmt.Sprintf("tcp://%s:%d", broker, port))
	opts.SetClientID("go_mqtt_client")
	opts.SetUsername("mpl")
	opts.SetPassword("mpl")
	opts.SetOrderMatters(false)          // Allow out of order messages (use this option unless in order delivery is essential)
	opts.ConnectTimeout = time.Second    // Minimal delays on connect
	opts.WriteTimeout = time.Millisecond // Minimal delays on writes
	opts.KeepAlive = 10                  // Keepalive every 10 seconds so we quickly detect network outages
	opts.PingTimeout = time.Second       // local broker so response should be quick

	opts.ConnectRetry = true
	opts.AutoReconnect = true

	opts.OnConnectionLost = func(cl mqtt.Client, err error) {
		fmt.Println("Connection to the MQTT broker was lost")
	}

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("Connection established to the MQTT broker")

		// Establish the subscription - doing this here means that it will happen every time a connection is established
		// (useful if opts.CleanSession is TRUE or the broker does not reliably store session data)
		t := c.Subscribe(topic, qos, h.handle)
		// the connection handler is called in a goroutine so blocking here would hot cause an issue. However as blocking
		// in other handlers does cause problems its best to just assume we should not block
		go func() {
			_ = t.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
			if t.Error() != nil {
				fmt.Printf("ERROR SUBSCRIBING: %s\n", t.Error())
			} else {
				fmt.Println("Subscribed to: ", topic)
			}
		}()
	}
	opts.OnReconnecting = func(mqtt.Client, *mqtt.ClientOptions) {
		fmt.Println("attempting to reconnect")
	}

	client := mqtt.NewClient(opts)

	// If using QOS2 and CleanSession = FALSE then messages may be transmitted to us before the subscribe completes.
	// Adding routes prior to connecting is a way of ensuring that these messages are processed
	client.AddRoute(topic, h.handle)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Connection is up")

	// Messages will be delivered asynchronously so we just need to wait for a signal to shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("Signal caught - exiting")
	client.Disconnect(1000)
	fmt.Println("Shutdown complete")
}
