package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	rabbit_url           = "amqp://guest:guest@192.169.253.156:5672/"
	xchange_name         = "debug-x"
	xchange_name_delayed = "debug-x-delayed"

	QUEUE_NAME         = "debug-q"
	QUEUE_DELAYED_NAME = "debug-q-delayed"
	// QUEUE_DELAYED_NAME = ""
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	// UseDefault()
	UseDelayedQueue()
}

func UseDefault() {
	exclusive := true
	if QUEUE_NAME != "" {
		exclusive = false
	}
	conn, err := amqp.Dial(rabbit_url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// err = ch.ExchangeDeclare(
	// 	xchange_name, // name
	// 	"fanout",          // type
	// 	true,              // durable
	// 	false,             // auto-deleted
	// 	false,             // internal
	// 	false,             // no-wait
	// 	nil,               // arguments
	// )
	// failOnError(err, "Failed to declare an exchange")

	q, err := ch.QueueDeclare(
		QUEUE_NAME, // name
		false,      // durable
		false,      // delete when unused
		exclusive,  // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,       // queue name
		"",           // routing key
		xchange_name, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			//d.Reject(true)
			d.Ack(false)
			time.Sleep(5 * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func UseDelayedQueue() {
	exclusive := true
	if QUEUE_DELAYED_NAME != "" {
		exclusive = false
	}
	conn, err := amqp.Dial(rabbit_url)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		QUEUE_DELAYED_NAME, // name
		false,              // durable
		false,              // delete when unused
		exclusive,          // exclusive
		false,              // no-wait
		nil,                // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.QueueBind(
		q.Name,               // queue name
		"",                   // routing key
		xchange_name_delayed, // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			//d.Reject(true)
			d.Ack(false)
			time.Sleep(5 * time.Second)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
