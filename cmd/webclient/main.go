// Nextjs Client giả lập.
// Mỗi "user" là 1 goroutine duyệt web: xem trang → click → có thể thêm giỏ →
// có thể checkout. Event được gom vào buffer rồi flush 1 lần lên /track,
// đúng cách navigator.sendBeacon hoạt động trong trình duyệt thật.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/huythanh/go-rabbitmq/internal/tracking"
	"github.com/huythanh/go-rabbitmq/internal/ui"
)

var out = ui.New("[web]", "cyan")

var pages = []string{
	"/", "/products/ao-thun-basic", "/products/quan-jean-slim",
	"/products/giay-sneaker", "/search?q=ao", "/cart", "/checkout",
}

func main() {
	endpoint := flag.String("endpoint", "http://localhost:8080/track", "Service 1 Tracking")
	users := flag.Int("users", 3, "số user duyệt web đồng thời")
	sessions := flag.Int("sessions", 2, "mỗi user duyệt bao nhiêu phiên rồi nghỉ")
	speed := flag.Duration("speed", 400*time.Millisecond, "thời gian giữa 2 hành vi")
	bad := flag.Float64("bad", 0, "tỉ lệ event rác cố tình gửi, 0.0 - 1.0")
	flag.Parse()

	out.Printf("mở %s tab, mỗi tab %d phiên → %s",
		ui.Bold(fmt.Sprint(*users)), *sessions, ui.Bold(*endpoint))
	if *bad > 0 {
		out.Printf("%s trộn %.0f%% event rác để xem service từ chối",
			ui.Yellow("chế độ nghịch:"), *bad*100)
	}

	var wg sync.WaitGroup
	for i := 1; i <= *users; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Lệch nhau chút cho log không dính cục.
			time.Sleep(time.Duration(n) * 150 * time.Millisecond)
			for s := 0; s < *sessions; s++ {
				browse(*endpoint, fmt.Sprintf("u_%04d", 1000+n), *speed, *bad)
			}
		}(i)
	}
	wg.Wait()

	out.Printf("%s tất cả tab đã đóng", ui.Green("xong."))
}

// browse mô phỏng 1 phiên duyệt web hoàn chỉnh.
func browse(endpoint, userID string, speed time.Duration, bad float64) {
	session := tracking.NewID("s")

	// 30% phiên là khách vãng lai chưa đăng nhập.
	if rand.Float64() < 0.3 {
		userID = ""
	}

	c := &collector{endpoint: endpoint, session: session, user: userID}

	out.Printf("%s phiên mới %s", ui.Bold(session), ui.Dim(label(userID)))

	// Landing
	page := pages[rand.Intn(4)]
	c.add(tracking.EvPageView, page, nil)
	time.Sleep(jitter(speed))

	// Đọc trang: scroll vài nhịp + click linh tinh
	for i := 0; i < 1+rand.Intn(3); i++ {
		c.add(tracking.EvScroll, page, map[string]any{"depth_pct": 25 * (i + 1)})
		time.Sleep(jitter(speed / 2))
	}
	c.add(tracking.EvClick, page, map[string]any{"target": "btn-xem-them"})

	// Flush lần 1 — giống lúc rời trang, sendBeacon bắn buffer đi.
	c.flush()
	time.Sleep(jitter(speed))

	// Có tìm kiếm không
	if rand.Float64() < 0.4 {
		c.add(tracking.EvSearch, "/search", map[string]any{"q": "ao thun", "results": rand.Intn(30)})
		time.Sleep(jitter(speed))
	}

	// Trộn event rác nếu được yêu cầu — để thấy service reject ra sao.
	if rand.Float64() < bad {
		c.addRaw(tracking.Event{
			ID:        tracking.NewID("ev"),
			Name:      "hack_the_planet", // không có trong whitelist
			Path:      page,
			SessionID: session,
			TS:        time.Now(),
		})
	}

	// Phễu: 40% thêm giỏ → trong đó 50% checkout → trong đó 60% trả tiền.
	// Con số thật của e-commerce cũng rơi rụng kiểu này.
	if rand.Float64() < 0.4 {
		sku := fmt.Sprintf("SKU-%03d", 100+rand.Intn(20))
		price := float64(150000 + rand.Intn(20)*10000)
		c.add(tracking.EvAddToCart, page, map[string]any{"sku": sku, "price": price})
		time.Sleep(jitter(speed))

		if rand.Float64() < 0.5 {
			c.add(tracking.EvPageView, "/checkout", nil)
			c.add(tracking.EvCheckout, "/checkout", map[string]any{"items": 1, "total": price})
			time.Sleep(jitter(speed))

			if rand.Float64() < 0.6 {
				c.add(tracking.EvPurchase, "/checkout/success", map[string]any{
					"order_id": tracking.NewID("ORD"), "total": price,
				})
			}
		}
	}

	c.flush()
}

// collector là buffer event phía trình duyệt.
type collector struct {
	endpoint string
	session  string
	user     string
	buf      []tracking.Event
}

func (c *collector) add(name, path string, props map[string]any) {
	c.addRaw(tracking.Event{
		ID:        tracking.NewID("ev"),
		Name:      name,
		Path:      path,
		SessionID: c.session,
		UserID:    c.user,
		TS:        time.Now(),
		Props:     props,
	})
}

func (c *collector) addRaw(e tracking.Event) {
	c.buf = append(c.buf, e)
	out.Step(c.session, "%s %s %-12s %s", ui.Dim("•"), ui.Bold(e.ID), e.Name, ui.Dim(e.Path))
}

// flush POST buffer lên Service 1 rồi in ack. Đây là mũi tên quay ngược
// từ Service 1 về Nextjs Client trong sơ đồ.
func (c *collector) flush() {
	if len(c.buf) == 0 {
		return
	}
	batch := tracking.Batch{SessionID: c.session, UserID: c.user, Events: c.buf}
	c.buf = nil

	body, _ := json.Marshal(batch)
	start := time.Now()

	resp, err := http.Post(c.endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		out.Printf("%s %s gửi %d event thất bại: %v",
			ui.Bold(c.session), ui.Red("✗"), len(batch.Events), err)
		return
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	took := time.Since(start)

	var ack tracking.Ack
	if err := json.Unmarshal(raw, &ack); err != nil {
		out.Printf("%s %s HTTP %d, ack không đọc được",
			ui.Bold(c.session), ui.Red("✗"), resp.StatusCode)
		return
	}

	status := ui.Green(fmt.Sprintf("%d", resp.StatusCode))
	msg := fmt.Sprintf("%s %s ↩ %s nhận %s", ui.Bold(c.session), ui.Dim("POST /track"),
		status, ui.Green(fmt.Sprint(ack.Accepted)))
	if n := len(ack.Rejected); n > 0 {
		msg += fmt.Sprintf(" %s %s", ui.Red("loại"), ui.Red(fmt.Sprint(n)))
	}
	out.Printf("%s %s", msg, ui.Dim(fmt.Sprintf("(%dms)", took.Milliseconds())))

	for _, rj := range ack.Rejected {
		out.Step(c.session, "%s %s %s", ui.Red("↳"), ui.Bold(rj.ID), ui.Red(rj.Reason))
	}
}

func label(userID string) string {
	if userID == "" {
		return "khách vãng lai"
	}
	return userID
}

// jitter làm nhịp bấm không đều, người thật không click đúng mỗi 400ms.
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}
