// Collector — consumer phía sau RabbitMQ.
// In từng event nhận được (ID trùng với log của client và service, để lần theo)
// và định kỳ vẽ bảng phễu chuyển đổi.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/huythanh/go-rabbitmq/internal/rabbitmq"
	"github.com/huythanh/go-rabbitmq/internal/tracking"
	"github.com/huythanh/go-rabbitmq/internal/ui"
	amqp "github.com/rabbitmq/amqp091-go"
)

const amqpURL = "amqp://admin:admin@localhost:5672/"

func main() {
	queue := flag.String("queue", rabbitmq.QueueTrackAll, "queue để consume")
	name := flag.String("name", "collector-1", "tên instance, để phân biệt log")
	work := flag.Duration("work", 80*time.Millisecond, "thời gian xử lý giả lập")
	failRate := flag.Float64("fail", 0, "tỉ lệ fail → DLQ, 0.0 - 1.0")
	prefetch := flag.Int("prefetch", 10, "số message chưa ack broker được phép giao")
	every := flag.Duration("summary", 5*time.Second, "chu kỳ in bảng phễu, 0 = tắt")
	flag.Parse()

	out := ui.New("["+*name+"]", "green")

	conn, ch, err := rabbitmq.Connect(amqpURL)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	defer ch.Close()

	if err := rabbitmq.Setup(ch); err != nil {
		log.Fatal(err)
	}
	if err := ch.Qos(*prefetch, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(*queue, *name, false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	out.Printf("nghe %s %s", ui.Bold(*queue),
		ui.Dim(fmt.Sprintf("(prefetch=%d, work=%v, fail=%.0f%%)", *prefetch, *work, *failRate*100)))

	st := &stats{counts: map[string]int{}, sessions: map[string]bool{}}

	// Bảng phễu chạy trên goroutine riêng, không chặn việc consume.
	stop := make(chan struct{})
	if *every > 0 {
		go func() {
			t := time.NewTicker(*every)
			defer t.Stop()
			for {
				select {
				case <-t.C:
					if s := st.render(); s != "" {
						ui.Print(s)
					}
				case <-stop:
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for d := range msgs {
			handle(out, st, d, *work, *failRate)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	out.Printf("shutdown: ngừng nhận việc mới, xử nốt việc đang làm")
	ch.Cancel(*name, false)
	<-done
	close(stop)

	if s := st.render(); s != "" {
		ui.Print(s)
	}
	out.Printf("thoát sạch")
}

func handle(out *ui.Logger, st *stats, d amqp.Delivery, work time.Duration, failRate float64) {
	var e tracking.Event
	if err := json.Unmarshal(d.Body, &e); err != nil {
		out.Printf("%s %s body hỏng → DLQ: %v", ui.Red("✗"), d.MessageId, err)
		d.Nack(false, false)
		return
	}

	tag := ""
	if d.Redelivered {
		// Message này từng được giao nhưng chưa ai Ack — worker trước chết giữa chừng.
		tag = " " + ui.Yellow("⟳ GIAO LẠI")
	}

	time.Sleep(work) // giả lập ghi vào ClickHouse / BigQuery

	if fail(failRate) {
		out.Printf("%s %s %-12s %s", ui.Red("✗"), ui.Bold(e.ID), e.Name, ui.Red("xử lý fail → DLQ"))
		d.Nack(false, false)
		st.record(e.Name, e.SessionID, false)
		return
	}

	who := e.UserID
	if who == "" {
		who = "khách"
	}
	out.Printf("%s %s %s %s %s %s%s",
		ui.Green("✓"),
		ui.Bold(e.ID),
		ui.Cyan(fmt.Sprintf("%-12s", e.Name)),
		ui.Dim(fmt.Sprintf("%-8s", e.SessionID)),
		ui.Dim(fmt.Sprintf("%-8s", who)),
		ui.Dim(e.Path),
		tag,
	)
	if len(e.Props) > 0 {
		out.Step(e.SessionID, "%s %s", ui.Dim("props"), ui.Dim(compact(e.Props)))
	}

	st.record(e.Name, e.SessionID, true)
	d.Ack(false) // broker xoá message khỏi queue tại đúng dòng này
}

// ---- thống kê ----

type stats struct {
	mu       sync.Mutex
	counts   map[string]int
	sessions map[string]bool
	failed   int
	dirty    bool // có gì mới kể từ lần vẽ trước không
}

func (s *stats) record(name, session string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty = true
	if !ok {
		s.failed++
		return
	}
	s.counts[name]++
	s.sessions[session] = true
}

// render vẽ phễu, trả rỗng nếu chưa có gì mới — bảng y hệt lặp lại mỗi 5s
// chỉ tổ đẩy log thật trôi khỏi màn hình.
func (s *stats) render() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.counts) == 0 || !s.dirty {
		return ""
	}
	s.dirty = false

	steps := []string{
		tracking.EvPageView, tracking.EvAddToCart,
		tracking.EvCheckout, tracking.EvPurchase,
	}

	// Mẫu số của % là page_view — "bao nhiêu lượt xem thì ra 1 đơn".
	// Nhưng queue tracking.funnel không hề bind page_view, nên base=0 và
	// mọi thanh bar sẽ rỗng tuếch. Khi đó lấy bước lớn nhất làm mẫu số
	// để thanh bar còn so sánh được với nhau; phần % thì ẩn đi vì vô nghĩa.
	base := s.counts[tracking.EvPageView]
	scale, hasPct := base, true
	if base == 0 {
		hasPct = false
		for _, st := range steps {
			if s.counts[st] > scale {
				scale = s.counts[st]
			}
		}
	}

	var lines []string
	for _, step := range steps {
		n := s.counts[step]
		frac := 0.0
		if scale > 0 {
			frac = float64(n) / float64(scale)
		}
		pct := ui.Dim("     —")
		if hasPct && step != tracking.EvPageView {
			pct = fmt.Sprintf("%5.1f%%", float64(n)/float64(base)*100)
		}
		lines = append(lines, fmt.Sprintf("%-12s %s %4d  %s",
			step, ui.Bar(frac, 20), n, pct))
	}

	// Các event còn lại gom xuống dưới, không nằm trong phễu.
	var others []string
	for k := range s.counts {
		switch k {
		case tracking.EvPageView, tracking.EvAddToCart, tracking.EvCheckout, tracking.EvPurchase:
		default:
			others = append(others, k)
		}
	}
	sort.Strings(others)
	if len(others) > 0 {
		lines = append(lines, ui.Dim("─── ngoài phễu ───"))
		for _, k := range others {
			lines = append(lines, fmt.Sprintf("%-12s %s %4d", k, ui.Dim(strings_repeat(20)), s.counts[k]))
		}
	}

	lines = append(lines, "")
	tail := fmt.Sprintf("%s phiên  %s event",
		ui.Bold(fmt.Sprint(len(s.sessions))), ui.Bold(fmt.Sprint(total(s.counts))))
	if s.failed > 0 {
		tail += fmt.Sprintf("  %s", ui.Red(fmt.Sprintf("%d rơi vào DLQ", s.failed)))
	}
	lines = append(lines, tail)

	return ui.Box("PHỄU CHUYỂN ĐỔI", lines)
}

func fail(rate float64) bool { return rate > 0 && rand.Float64() < rate }

func total(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

func compact(m map[string]any) string {
	b, _ := json.Marshal(m)
	return string(b)
}

func strings_repeat(n int) string {
	s := make([]byte, n)
	for i := range s {
		s[i] = ' '
	}
	return string(s)
}
