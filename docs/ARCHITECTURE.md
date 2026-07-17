# Hệ thống tracking hoạt động thế nào

## 1. Toàn cảnh

Ba tiến trình rời nhau, nói chuyện qua HTTP và AMQP. Không đứa nào gọi thẳng đứa nào.

```mermaid
flowchart LR
    C["🖥️ Nextjs Client<br/><i>cmd/webclient</i><br/>gom event, bắn theo batch"]
    S["⚙️ Service 1 Tracking<br/><i>cmd/tracking</i><br/>HTTP :8080"]
    X{{"📮 exchange: tracking<br/>kiểu topic"}}
    Q1[("📦 tracking.all<br/>bind: event.#")]
    Q2[("📦 tracking.funnel<br/>bind: 3 event có tiền")]
    K1["📊 Collector<br/><i>cmd/collector</i><br/>vẽ phễu"]
    K2["💰 Collector funnel<br/><i>cmd/collector</i>"]

    C -->|"POST /track<br/>batch JSON"| S
    S -.->|"202 accepted=N<br/><b>trả ngay, không chờ</b>"| C
    S -->|"publish<br/>key: event.page_view"| X
    X -->|"event.#"| Q1
    X -->|"event.add_to_cart<br/>event.checkout<br/>event.purchase"| Q2
    Q1 --> K1
    Q2 --> K2

    style C fill:#0ea5e9,color:#fff
    style S fill:#f59e0b,color:#fff
    style X fill:#8b5cf6,color:#fff
    style K1 fill:#22c55e,color:#fff
    style K2 fill:#22c55e,color:#fff
```

**Điểm mấu chốt:** mũi tên đứt nét (`202 accepted`) quay về client **trước khi** collector
xử lý xong. Đó là toàn bộ lý do RabbitMQ có mặt ở đây.

---

## 2. Một batch đi qua hệ thống ra sao

```mermaid
sequenceDiagram
    autonumber
    participant C as 🖥️ Nextjs Client
    participant S as ⚙️ Service 1
    participant R as 📮 RabbitMQ
    participant K as 📊 Collector

    Note over C: user xem trang, scroll, click
    C->>C: gom 4 event vào buffer

    C->>S: POST /track (batch 4 event)
    S->>S: validate từng event<br/>(name có trong whitelist?)
    S->>R: publish ×4, key = event.{tên}
    R-->>S: ok
    S-->>C: 202 { accepted: 4 }

    Note over C,S: 🏁 Client XONG ở đây (~7ms).<br/>User không phải chờ gì thêm.

    R->>K: deliver event 1
    K->>K: ghi vào ClickHouse (80ms)
    K-->>R: Ack → broker xoá message
    R->>K: deliver event 2
    K->>K: ...

    Note over R,K: Collector chậm cỡ nào cũng kệ.<br/>Message nằm trong queue chờ sẵn.
```

Nếu **không có** RabbitMQ, Service 1 phải tự ghi ClickHouse → client chờ 4×80ms = 320ms
thay vì 7ms. Và ClickHouse sập là mất luôn event.

---

## 3. Topic exchange lọc message thế nào

Service 1 chỉ publish **một lần** cho mỗi event, vào đúng một exchange.
Việc message nhân bản đi đâu là do **binding** quyết định, service không cần biết.

```mermaid
flowchart TD
    E1["event.page_view"] --> X{{"exchange: tracking<br/>topic"}}
    E2["event.scroll"] --> X
    E3["event.add_to_cart"] --> X
    E4["event.purchase"] --> X

    X --> Q1[("tracking.all<br/><code>event.#</code>")]
    X --> Q2[("tracking.funnel<br/><code>event.add_to_cart</code><br/><code>event.checkout</code><br/><code>event.purchase</code>")]

    style X fill:#8b5cf6,color:#fff
    style Q1 fill:#1e293b,color:#fff
    style Q2 fill:#1e293b,color:#fff
```

| Routing key         | `tracking.all`<br/>`event.#` | `tracking.funnel`<br/>3 key cụ thể |
|---------------------|:---------------------------:|:----------------------------------:|
| `event.page_view`   | ✅ | ❌ |
| `event.scroll`      | ✅ | ❌ |
| `event.click`       | ✅ | ❌ |
| `event.search`      | ✅ | ❌ |
| `event.add_to_cart` | ✅ | ✅ |
| `event.checkout`    | ✅ | ✅ |
| `event.purchase`    | ✅ | ✅ |

- `#` = khớp **nhiều từ** → `event.#` nuốt tất, kể cả event bạn thêm sau này.
- Một queue bind **nhiều key** = quan hệ **HOẶC**, không phải VÀ.
- Cùng một message vào 2 queue = **2 bản copy độc lập**. `tracking.funnel` Ack hay Nack
  không ảnh hưởng gì tới bản nằm ở `tracking.all`.

> Đây là con số thật đo được khi chạy 4 tab × 3 phiên:
> `tracking.all` nhận **63** event, `tracking.funnel` nhận đúng **8** (6 add_to_cart + 1 checkout + 1 purchase).

---

## 4. Message hỏng đi đâu — Dead Letter Queue

```mermaid
flowchart LR
    Q[("tracking.all<br/><code>x-dead-letter-exchange:<br/>events.dlx</code>")]
    K["Collector"]
    DLX{{"events.dlx<br/>fanout"}}
    D[("events.dead<br/>🪦 nghĩa địa")]

    Q -->|deliver| K
    K -->|"✅ Ack<br/>xong việc"| OK(["broker xoá message"])
    K -->|"❌ Nack(requeue=false)<br/>body hỏng / xử lý fail"| DLX
    DLX --> D
    D -->|"đọc sau để điều tra"| H["👨‍🔧 người"]

    style DLX fill:#ef4444,color:#fff
    style D fill:#7f1d1d,color:#fff
    style OK fill:#22c55e,color:#fff
```

Ba đường ra của một message, không có đường thứ tư:

| Collector làm gì | Kết quả |
|---|---|
| `Ack()` | broker xoá message, xong đời |
| `Nack(requeue=false)` | bay sang `events.dlx` → nằm ở `events.dead` |
| **chết giữa chừng, chưa Ack** | broker giao lại cho worker khác (log hiện `⟳ GIAO LẠI`) |

Trường hợp 3 là lý do phải `Ack` **sau** khi làm xong việc, không phải trước.

---

## 5. Nhiều collector = chia việc, không phải nhân bản

Chỗ này rất hay nhầm.

```mermaid
flowchart LR
    subgraph one ["❌ 2 collector CÙNG 1 queue → chia nhau"]
        direction LR
        QA[("tracking.all<br/>4 message")]
        QA -->|"ev_01, ev_03"| CA["collector c1"]
        QA -->|"ev_02, ev_04"| CB["collector c2"]
    end

    subgraph two ["✅ 2 queue KHÁC nhau → mỗi đứa nhận đủ"]
        direction LR
        XB{{"exchange tracking"}}
        XB --> QB1[("tracking.all<br/>4 message")]
        XB --> QB2[("tracking.funnel<br/>4 message")]
        QB1 --> CC["collector all"]
        QB2 --> CD["collector funnel"]
    end
```

- Muốn **chạy nhanh hơn** (chia tải): thêm collector vào cùng 1 queue.
- Muốn **xử lý khác nhau** (analytics vs email): tạo queue mới, bind riêng.

`prefetch` quyết định chia tải có đều không:

```mermaid
flowchart LR
    P1["prefetch=1"] --> R1["broker giao 1 việc,<br/>Ack xong mới giao tiếp<br/>→ chia đều thật"]
    P10["prefetch=10"] --> R10["1 worker ôm 10 việc,<br/>worker kia ngồi chơi<br/>→ nhanh nhưng lệch"]
```

---

## 6. Lần theo 1 event qua cả 3 log

Mỗi event mang `ev_xxxx` + `s_xxxx` (session), in ra ở **cả ba** tiến trình.
Grep đúng 1 ID là thấy trọn đường đi:

```mermaid
flowchart TD
    A["🖥️ [web] s_0e5b • ev_761a page_view /products/quan-jean-slim<br/><i>trình duyệt ghi nhận, nhét vào buffer</i>"]
    B["🖥️ [web] s_0e5b POST /track ↩ 202 nhận 4 (7ms)<br/><i>flush buffer, service trả ack</i>"]
    C["⚙️ [svc] s_0e5b ✓ ev_761a page_view → tracking / event.page_view<br/><i>validate xong, đẩy vào exchange</i>"]
    D["📊 [all] ✓ ev_761a page_view s_0e5b u_1001 /products/quan-jean-slim<br/><i>collector lấy ra, xử lý, Ack</i>"]

    A --> B --> C --> D

    style A fill:#0ea5e9,color:#fff
    style B fill:#0ea5e9,color:#fff
    style C fill:#f59e0b,color:#fff
    style D fill:#22c55e,color:#fff
```

```bash
# thấy trọn vòng đời của 1 event
go run ./cmd/webclient -users 2 2>&1 | grep ev_761a
```

---

## 7. Bản đồ code

```mermaid
flowchart TD
    T["internal/tracking/event.go<br/>Event, Batch, Ack, whitelist<br/><b>contract chung của cả 3</b>"]
    R["internal/rabbitmq/topology.go<br/>Setup() khai báo exchange + queue + bind<br/><b>idempotent, ai khởi động cũng gọi</b>"]
    U["internal/ui/ui.go<br/>màu, khung, thanh bar<br/>khoá mutex chống log rối"]

    C["cmd/webclient"] --> T
    S["cmd/tracking"] --> T
    K["cmd/collector"] --> T
    C --> U
    S --> U & R
    K --> U & R

    style T fill:#8b5cf6,color:#fff
    style R fill:#8b5cf6,color:#fff
    style U fill:#8b5cf6,color:#fff
```

`Setup()` **idempotent** — cả service lẫn collector đều gọi lúc khởi động, chạy bao
nhiêu lần cũng ra cùng kết quả. Nhờ vậy khởi động thứ tự nào cũng được.
