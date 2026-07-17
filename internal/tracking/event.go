// Package tracking định nghĩa contract giữa Nextjs client và Service 1.
// Client, service và collector đều import package này — sửa 1 chỗ, cả 3 cùng đổi.
package tracking

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// Tên event. Đây là danh sách whitelist — service từ chối tên lạ,
// nếu không routing key sẽ nở ra vô hạn và queue bind không kiểm soát nổi.
const (
	EvPageView  = "page_view"
	EvClick     = "click"
	EvScroll    = "scroll"
	EvSearch    = "search"
	EvAddToCart = "add_to_cart"
	EvCheckout  = "checkout"
	EvPurchase  = "purchase"
)

var known = map[string]bool{
	EvPageView: true, EvClick: true, EvScroll: true, EvSearch: true,
	EvAddToCart: true, EvCheckout: true, EvPurchase: true,
}

func IsKnown(name string) bool { return known[name] }

// Event là 1 hành vi người dùng. Trình duyệt gom nhiều event rồi bắn 1 lần
// (giống navigator.sendBeacon), nên nó luôn đi trong Batch.
type Event struct {
	ID        string         `json:"id"`                // ev_xxxx — theo dấu xuyên suốt client → svc → queue
	Name      string         `json:"name"`              // page_view, click, ...
	Path      string         `json:"path"`              // URL trang lúc event xảy ra
	SessionID string         `json:"session_id"`        // s_xxxx
	UserID    string         `json:"user_id,omitempty"` // rỗng = khách vãng lai
	TS        time.Time      `json:"ts"`
	Props     map[string]any `json:"props,omitempty"`
}

// Batch là payload client POST lên /track.
type Batch struct {
	SessionID string  `json:"session_id"`
	UserID    string  `json:"user_id,omitempty"`
	Events    []Event `json:"events"`
}

// Reject giải thích vì sao 1 event bị loại. Client cần biết để đừng gửi lại.
type Reject struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// Ack là response của /track — mũi tên quay ngược về Nextjs Client trong sơ đồ.
type Ack struct {
	Accepted int      `json:"accepted"`
	Rejected []Reject `json:"rejected,omitempty"`
	TookMS   int64    `json:"took_ms"`
}

// RoutingKey biến tên event thành routing key của topic exchange.
// Dấu "." là ký tự phân tách của topic, mà tên event dùng "_" nên an toàn.
func RoutingKey(name string) string { return "event." + name }

// Validate trả về lý do loại, rỗng nghĩa là hợp lệ.
func (e Event) Validate() string {
	switch {
	case e.Name == "":
		return "thiếu name"
	case !IsKnown(e.Name):
		return fmt.Sprintf("name %q không nằm trong whitelist", e.Name)
	case e.SessionID == "":
		return "thiếu session_id"
	case e.TS.IsZero():
		return "thiếu ts"
	}
	return ""
}

// ID ngắn cho dễ đọc trên terminal. Không phải UUID vì mục đích ở đây là
// mắt người nhìn 1 phát ra ngay, không phải chống trùng toàn cầu.
func NewID(prefix string) string {
	b := make([]byte, 2)
	rand.Read(b)
	return prefix + "_" + hex.EncodeToString(b)
}
