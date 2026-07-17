// Service 1 — Tracking.
// Nhận batch hành vi từ Nextjs client qua HTTP, validate, đẩy vào RabbitMQ,
// rồi trả ack ngay. Client KHÔNG chờ consumer xử lý xong — đó là lý do
// có RabbitMQ ở giữa.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/huythanh/go-rabbitmq/internal/rabbitmq"
	"github.com/huythanh/go-rabbitmq/internal/tracking"
	"github.com/huythanh/go-rabbitmq/internal/ui"
	amqp "github.com/rabbitmq/amqp091-go"
)

const amqpURL = "amqp://admin:admin@localhost:5672/"

var out = ui.New("[svc]", "yellow")

type server struct {
	ch *amqp.Channel
	mu sync.Mutex // amqp.Channel KHÔNG an toàn cho nhiều goroutine publish cùng lúc,
	// mà mỗi request HTTP là 1 goroutine → phải khoá.
}

func main() {
	addr := flag.String("addr", ":8080", "địa chỉ HTTP lắng nghe")
	flag.Parse()

	conn, ch, err := rabbitmq.Connect(amqpURL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	defer ch.Close()

	if err := rabbitmq.Setup(ch); err != nil {
		log.Fatal(err)
	}

	s := &server{ch: ch}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /track", s.handleTrack)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	srv := &http.Server{Addr: *addr, Handler: cors(mux)}

	go func() {
		out.Printf("Service 1 Tracking nghe trên %s → exchange %s",
			ui.Bold(*addr), ui.Bold(rabbitmq.TrackingX))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	out.Printf("shutdown: xử nốt request đang chạy...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	out.Printf("thoát sạch")
}

func (s *server) handleTrack(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var b tracking.Batch
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		out.Printf("%s body hỏng: %v", ui.Red("400"), err)
		http.Error(w, "json không hợp lệ", http.StatusBadRequest)
		return
	}

	who := b.UserID
	if who == "" {
		who = ui.Dim("khách")
	}
	out.Printf("%s batch %d event từ %s %s",
		ui.Bold(b.SessionID), len(b.Events), who, ui.Dim("("+r.RemoteAddr+")"))

	ack := tracking.Ack{}
	for _, e := range b.Events {
		// Event trong batch thừa hưởng session/user của batch nếu để trống.
		if e.SessionID == "" {
			e.SessionID = b.SessionID
		}
		if e.UserID == "" {
			e.UserID = b.UserID
		}

		if reason := e.Validate(); reason != "" {
			out.Step(b.SessionID, "%s %s %-12s %s", ui.Red("✗"), ui.Bold(e.ID), e.Name, ui.Red(reason))
			ack.Rejected = append(ack.Rejected, tracking.Reject{ID: e.ID, Reason: reason})
			continue
		}

		key := tracking.RoutingKey(e.Name)
		if err := s.publish(r.Context(), key, e); err != nil {
			out.Step(b.SessionID, "%s %s %-12s %s", ui.Red("✗"), ui.Bold(e.ID), e.Name, ui.Red("publish lỗi: "+err.Error()))
			ack.Rejected = append(ack.Rejected, tracking.Reject{ID: e.ID, Reason: "broker không nhận"})
			continue
		}

		out.Step(b.SessionID, "%s %s %-12s %s %s", ui.Green("✓"), ui.Bold(e.ID),
			e.Name, ui.Dim("→ "+rabbitmq.TrackingX+" /"), ui.Cyan(key))
		ack.Accepted++
	}

	ack.TookMS = time.Since(start).Milliseconds()

	// 202 chứ không phải 200: "tôi nhận rồi, sẽ xử lý sau", đúng bản chất async.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(ack)
}

func (s *server) publish(ctx context.Context, key string, e tracking.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ch.PublishWithContext(ctx, rabbitmq.TrackingX, key, false, false,
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         body,
			DeliveryMode: amqp.Persistent, // sống sót qua broker restart
			MessageId:    e.ID,
			Timestamp:    e.TS,
		})
}

// cors để nếu bạn cắm Nextjs thật ở localhost:3000 thì trình duyệt không chặn.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
