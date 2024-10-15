package main

import (
	"log"

	"github.com/streadway/amqp"
)

const (
	rabbit_url           = "amqp://guest:guest@192.169.253.156:5672/"
	xchange_name         = "debug-x"
	xchange_name_delayed = "debug-x-delayed"
)

func failOnError(er error, msg string) {
	if er != nil {
		log.Fatalf("%s %s", msg, er)
	}
}

func main() {
	UseDelayedFanout()
}

func UseFanout() {
	conn, er := amqp.Dial(rabbit_url)
	failOnError(er, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, er := conn.Channel()
	failOnError(er, "Failed to open channel")
	defer ch.Close()

	er = ch.ExchangeDeclare(
		xchange_name, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	failOnError(er, "Failed to declare an exchange")

	body := "Hello World!"
	er = ch.Publish(
		xchange_name, // exchange
		"",           // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(er, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}

func UseDelayedFanout() {
	conn, er := amqp.Dial(rabbit_url)
	failOnError(er, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, er := conn.Channel()
	failOnError(er, "Failed to open channel")
	defer ch.Close()

	er = ch.ExchangeDeclare(
		xchange_name_delayed, // name
		"x-delayed-message",  //** type : untuk menggunakan delay, type menggunakan x-delayed-message
		true,                 // durable
		false,                // auto-deleted
		false,                // internal
		false,                // no-wait
		amqp.Table{ //** arguments : tambahkan argumen berikut untuk tipe fanout
			"x-delayed-type": "fanout",
		},
	)
	failOnError(er, "Failed to declare an exchange")

	body := "Delay Message >> Hello World! "
	//** Header untuk konfigurasi durasi delay, set 0 jika tidak menggunakan delay
	headers := amqp.Table{
		"x-delay": int32(5000), // Delay 5 detik (dalam milidetik)
	}

	er = ch.Publish(
		xchange_name_delayed, // exchange
		"",                   // routing key
		false,                // mandatory
		false,                // immediate
		amqp.Publishing{
			Headers:     headers, // Tambahkan header
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(er, "Failed to publish a message")

	log.Printf(" [x] Sent %s", body)
}
