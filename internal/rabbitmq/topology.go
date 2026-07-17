package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ---- Exchanges (nơi producer bắn message vào) ----
const (
	Exchange    = "events"     // topic exchange chính
	DeadLetterX = "events.dlx" // fanout exchange hứng message hỏng
)

// ---- Queues (nơi message nằm chờ consumer) ----
const (
	QueueAnalytics = "events.analytics" // bind "order.*"       -> nhận mọi event order
	QueueEmail     = "events.email"     // bind "order.created" -> chỉ nhận lúc tạo đơn
	QueueDead      = "events.dead"      // bind vào DLX          -> nghĩa địa
)

// Connect mở TCP connection + 1 channel.
// Connection là kết nối thật; Channel là kênh ảo chạy trên nó.
func Connect(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", url, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}

	return conn, ch, nil
}

// Setup khai báo toàn bộ topology. Idempotent — producer và consumer
// đều gọi lúc khởi động, chạy bao nhiêu lần cũng ra cùng kết quả.
//
// LƯU Ý THỨ TỰ THAM SỐ (chỗ hay sai nhất):
//
//	ExchangeDeclare(name, kind, durable, autoDelete, internal, noWait, args)
//	QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
//	QueueBind(queueName, routingKey, exchangeName, noWait, args)
//	                ↑1         ↑2           ↑3
func Setup(ch *amqp.Channel) error {
	// 1. Exchange chính. Producer chỉ biết mỗi thằng này.
	if err := ch.ExchangeDeclare(
		Exchange, // name
		"topic",  // kind: routing theo pattern (* = 1 từ, # = nhiều từ)
		true,     // durable: sống sót qua broker restart
		false,    // autoDelete
		false,    // internal
		false,    // noWait
		nil,      // args
	); err != nil {
		return fmt.Errorf("declare exchange %q: %w", Exchange, err)
	}

	// 2. Dead-letter exchange. fanout = ai bind vào cũng nhận, kệ routing key.
	if err := ch.ExchangeDeclare(
		DeadLetterX, "fanout", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("declare exchange %q: %w", DeadLetterX, err)
	}

	// 3. Queue nghĩa địa + bind vào DLX (routing key rỗng vì fanout bỏ qua nó).
	if _, err := ch.QueueDeclare(
		QueueDead, true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("declare queue %q: %w", QueueDead, err)
	}
	if err := ch.QueueBind(
		QueueDead,   // queue
		"",          // routing key
		DeadLetterX, // exchange
		false, nil,
	); err != nil {
		return fmt.Errorf("bind queue %q -> exchange %q: %w", QueueDead, DeadLetterX, err)
	}

	// Message bị Nack(requeue=false) ở queue nào có arg này sẽ tự bay sang DLX.
	dlqArgs := amqp.Table{"x-dead-letter-exchange": DeadLetterX}

	// 4. Queue analytics — quan tâm MỌI event của order.
	if _, err := ch.QueueDeclare(
		QueueAnalytics, true, false, false, false, dlqArgs,
	); err != nil {
		return fmt.Errorf("declare queue %q: %w", QueueAnalytics, err)
	}
	if err := ch.QueueBind(
		QueueAnalytics, // queue
		"order.*",      // routing key: order.created, order.paid, order.cancelled...
		Exchange,       // exchange
		false, nil,
	); err != nil {
		return fmt.Errorf("bind queue %q -> exchange %q: %w", QueueAnalytics, Exchange, err)
	}

	// 5. Queue email — CHỈ quan tâm order.created.
	// Cùng exchange, cùng message, nhưng bộ lọc khác nhau.
	if _, err := ch.QueueDeclare(
		QueueEmail, true, false, false, false, dlqArgs,
	); err != nil {
		return fmt.Errorf("declare queue %q: %w", QueueEmail, err)
	}
	if err := ch.QueueBind(
		QueueEmail,      // queue
		"order.created", // routing key
		Exchange,        // exchange
		false, nil,
	); err != nil {
		return fmt.Errorf("bind queue %q -> exchange %q: %w", QueueEmail, Exchange, err)
	}

	return nil
}
