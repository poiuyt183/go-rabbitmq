package main

import (
	"encoding/json"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/huythanh/go-rabbitmq/internal/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

const amqpURL = "amqp://admin:admin@localhost:5672/"

func main() {
	queue := flag.String("queue", rabbitmq.QueueAnalytics, "queue để consume")
	name := flag.String("name", "worker-1", "tên instance, để phân biệt log")
	work := flag.Duration("work", 500*time.Millisecond, "thời gian xử lý giả lập")
	failRate := flag.Float64("fail", 0, "tỉ lệ fail, 0.0 - 1.0")
	prefetch := flag.Int("prefetch", 1, "số message chưa ack broker được phép giao")
	flag.Parse()

	conn, ch, err := rabbitmq.Connect(amqpURL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	defer ch.Close()

	// Bắt buộc trước Consume. Lỗi protocol sẽ đóng channel,
	// bỏ qua err ở đây là mọi lệnh sau đều fail khó hiểu.
	if err := rabbitmq.Setup(ch); err != nil {
		log.Fatal(err)
	}

	// prefetch=1  → broker giao 1 việc, ack xong mới giao tiếp (load balance thật)
	// prefetch=100 → 1 worker ôm hết, worker kia ngồi chơi
	if err := ch.Qos(*prefetch, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		*queue, // queue — phải là tên QUEUE, không phải exchange
		*name,  // consumer tag
		false,  // autoAck=false → tự Ack thủ công
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // args
	)
	if err != nil {
		log.Fatal(err)
	}

	logger := log.New(os.Stdout, "["+*name+"] ", log.Ltime)
	logger.Printf("listening on %s (prefetch=%d, work=%v, fail=%.0f%%)",
		*queue, *prefetch, *work, *failRate*100)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range msgs {
			handle(logger, d, *work, *failRate)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Println("shutdown: ngừng nhận việc mới, xử nốt việc đang làm")
	ch.Cancel(*name, false) // msgs channel sẽ đóng, vòng for thoát
	<-done
	logger.Println("thoát sạch")
}

func handle(logger *log.Logger, d amqp.Delivery, work time.Duration, failRate float64) {
	var evt map[string]any
	if err := json.Unmarshal(d.Body, &evt); err != nil {
		logger.Printf("✗ %s body hỏng, vứt thẳng vào DLQ: %v", d.MessageId, err)
		d.Nack(false, false)
		return
	}

	tag := ""
	if d.Redelivered {
		tag = " ⟳ REDELIVERED" // message này từng được giao nhưng chưa ai Ack
	}
	logger.Printf("nhận %s [%s]%s — xử lý %v...", d.MessageId, d.RoutingKey, tag, work)

	time.Sleep(work) // giả lập việc nặng

	if rand.Float64() < failRate {
		logger.Printf("✗ %s FAIL → DLQ", d.MessageId)
		d.Nack(false, false) // requeue=false → rơi sang dead-letter exchange
		return
	}

	logger.Printf("✓ %s xong", d.MessageId)
	d.Ack(false) // broker xoá message khỏi queue tại đúng dòng này
}
