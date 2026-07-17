package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/huythanh/go-rabbitmq/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type OrderEvent struct {
	OrderID string    `json:"order_id"`
	Total   float64   `json:"total"`
	At      time.Time `json:"at"`
}

func main() {
	n := flag.Int("n", 1, "số message gửi")
	key := flag.String("key", "order.created", "routing key")
	flag.Parse()

	conn, ch, err := rabbitmq.Connect("amqp://admin:admin@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	defer ch.Close()

	if err := rabbitmq.Setup(ch); err != nil {
		log.Fatal(err)
	}

	for i := 1; i <= *n; i++ {
		id := fmt.Sprintf("ORD-%03d", i)
		body, _ := json.Marshal(OrderEvent{
			OrderID: id,
			Total:   float64(100000 + i*1000),
			At:      time.Now(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := ch.PublishWithContext(ctx, rabbitmq.Exchange, *key, false, false,
			amqp.Publishing{
				ContentType:  "application/json",
				Body:         body,
				DeliveryMode: amqp.Persistent,
				MessageId:    id,
			})
		cancel()

		if err != nil {
			log.Printf("publish %s failed: %v", id, err)
			continue
		}
		log.Printf("→ published %s [%s]", id, *key)
	}
}
