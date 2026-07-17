# go-rabbitmq — luồng tracking hành vi người dùng

```
  Nextjs Client  ──POST /track──▶  Service 1        ──publish──▶  RabbitMQ  ──▶  Collector
  (cmd/webclient)                  Tracking                       exchange       (cmd/collector)
        ◀────── 202 accepted=N ──── (cmd/tracking)                "tracking"
```

Client **không** chờ collector xử lý xong. Service 1 nhận → đẩy vào queue → trả `202` ngay.
Đó chính là lý do có RabbitMQ đứng giữa.

## Chạy thử

Cần RabbitMQ ở `localhost:5672` (user `admin` / `admin`).

Mở 3 terminal:

```bash
# 1. Service 1 — nhận HTTP, publish vào exchange "tracking"
go run ./cmd/tracking

# 2. Collector — consume và vẽ phễu
go run ./cmd/collector

# 3. Nextjs client giả lập — 4 tab, mỗi tab 3 phiên duyệt web
go run ./cmd/webclient -users 4 -sessions 3
```

## Đọc output thế nào

Mỗi event có **1 ID ngắn (`ev_xxxx`) hiện ở cả 3 log**, kèm session `s_xxxx`.
Muốn lần theo 1 event xuyên suốt hệ thống thì grep đúng ID đó:

```
[web]  13:32:26 │   s_0e5b  • ev_761a page_view  /products/quan-jean-slim   ← trình duyệt ghi nhận
[svc]  13:32:26 │   s_0e5b  ✓ ev_761a page_view  → tracking / event.page_view  ← đẩy vào exchange
[all]  13:32:26 │ ✓ ev_761a page_view  s_0e5b  u_1001  /products/quan-jean-slim  ← consumer xử lý
```

Cứ 5s collector in bảng phễu (chỉ in khi có số liệu mới):

```
┌─ PHỄU CHUYỂN ĐỔI ────────────────────
│ page_view    ████████████████████   13       —
│ add_to_cart  █████████░░░░░░░░░░░    6   46.2%
│ checkout     ██░░░░░░░░░░░░░░░░░░    1    7.7%
│ purchase     ██░░░░░░░░░░░░░░░░░░    1    7.7%
│ ─── ngoài phễu ───
│ click                               12
│ scroll                              25
│ 12 phiên  63 event
└──────────────────────────────────────
```

Đặt `NO_COLOR=1` nếu muốn log sạch màu để pipe ra file.

## Topology

| Queue             | Bind vào exchange `tracking`                  | Nhận gì            |
|-------------------|-----------------------------------------------|--------------------|
| `tracking.all`    | `event.#`                                     | mọi hành vi        |
| `tracking.funnel` | `event.add_to_cart`, `.checkout`, `.purchase` | chỉ event có tiền  |

Cùng 1 message, hai queue lọc khác nhau — đó là điểm ăn tiền của topic exchange.
Cả hai queue đều gắn `x-dead-letter-exchange: events.dlx`, message xử lý fail rơi vào `events.dead`.

## Vọc thêm

```bash
# Xem topic exchange lọc ra sao: queue này chỉ nhận add_to_cart/checkout/purchase
go run ./cmd/collector -queue tracking.funnel -name funnel

# Xem service từ chối event rác (tên không nằm trong whitelist)
go run ./cmd/webclient -bad 0.4

# Xem message hỏng rơi vào DLQ
go run ./cmd/collector -fail 0.3
go run ./cmd/collector -queue events.dead -name nghiadia

# Hai collector cùng queue = chia việc (competing consumers).
# Chạy 2 cửa sổ, để ý mỗi event chỉ 1 đứa nhận:
go run ./cmd/collector -name c1
go run ./cmd/collector -name c2
```

> Lưu ý: nhiều collector cùng nghe 1 queue sẽ **chia nhau** message chứ không phải
> mỗi đứa nhận 1 bản. Nếu thấy số liệu phễu thiếu, kiểm tra xem có tiến trình
> collector cũ còn sót lại không.

## Phần order (có sẵn từ trước)

```bash
go run ./cmd/producer -n 5
go run ./cmd/consumer -queue events.analytics
```
