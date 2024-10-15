package main

import (
	"log"

	"github.com/streadway/amqp"
)

func failOnError(er error, msg string) {
	if er != nil {
		log.Fatalf("%s %s", msg, er)
	}
}

func main() {
	conn, er := amqp.Dial("amqp://randy:123456@localhost:5672/")
	failOnError(er, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, er := conn.Channel()
	failOnError(er, "Failed to open channel")
	defer ch.Close()

	er = ch.ExchangeDeclare(
		"transaction_log", // name
		"fanout",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	failOnError(er, "Failed to declare an exchange")

	body := "Hello World!"
	er = ch.Publish(
		"transaction_log", // exchange
		"",                // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(er, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}
