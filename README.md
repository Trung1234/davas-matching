# DAVAS Matching

Nền tảng **ghép nối, xếp lịch 1:1 và đo lường** cho sự kiện đầu tư khởi nghiệp DAVAS
(đề #10, AI Riser Vietnam 2026), kết hợp mục tiêu học backend: Redis concurrency,
Kafka retry, Idempotency, PostgreSQL Partitioning, Logical Replication, Migration.

> Ghi chú: các quy tắc nghiệp vụ và KPI trong tài liệu là giả định hợp lý, cần xác nhận với BTC/mentor.

---

## 1. Tổng quan

### Bài toán
| Vấn đề | Giải pháp |
|---|---|
| Ghép nối thủ công, khó đúng khẩu vị và giai đoạn đầu tư | AI (Gemini) chấm điểm tương thích kèm giải thích |
| Lịch Business Matching 1:1 dễ trùng, sai đối tượng | Đặt slot an toàn: Redis lock + ràng buộc DB |
| Cần theo dõi kết nối sau sự kiện | Pipeline follow-up có trạng thái + nhắc việc |
| Dữ liệu hồ sơ phân tán, chưa chuẩn hóa | Import Excel, schema chuẩn |
| Chưa đo lường được hiệu quả kết nối | Dashboard KPI trên replica riêng |

### Vai trò người dùng
- **Startup**: khai hồ sơ, xem gợi ý, xác nhận quan tâm, xem lịch.
- **Nhà đầu tư / VC / Angel**: khai khẩu vị, duyệt gợi ý, ghi kết quả cuộc gặp.
- **BTC (Admin)**: import dữ liệu, cấu hình slot, theo dõi dashboard.

---

## 2. Tech stack

### Backend (Python 3.12)
| Mảng | Công nghệ |
|---|---|
| API | FastAPI, Uvicorn, Pydantic v2 |
| ORM / DB driver | SQLAlchemy 2 (async), asyncpg |
| Migration | Alembic |
| Cache / Lock / Rate limit | Redis 7 (redis-py, Lua script) |
| Messaging | Kafka (KRaft) + aiokafka |
| AI | Gemini qua `google-genai` |
| Config | pydantic-settings |
| Test | pytest, pytest-asyncio, httpx, Locust (load test) |
| Quản lý package | uv |

### Frontend
| Mảng | Công nghệ |
|---|---|
| Build tool | Vite |
| UI | React 18 + TypeScript |
| Routing | React Router |
| Data fetching | TanStack Query |
| Form / validate | React Hook Form + Zod |
| UI kit | Tailwind CSS (+ shadcn/ui tùy chọn) |
| Biểu đồ | Recharts |
| Lịch | FullCalendar hoặc react-big-calendar |

### Hạ tầng
| Mảng | Công nghệ |
|---|---|
| Container | Docker Compose |
| Database | PostgreSQL 16 (primary + replica) |
| Quan sát | Prometheus + Grafana, Kafka UI, RedisInsight |
| Triển khai thi | Cloud Run (API), Cloud SQL, Memorystore (bản gọn) |

---

## 3. Kiến trúc tổng thể

```mermaid
flowchart LR
    subgraph FE["Frontend: React + Vite"]
        UI["Web App"]
    end

    UI -->|"REST / JSON"| API["FastAPI API"]

    API -->|"lock, rate limit"| Redis[("Redis")]
    API -->|"ghi meeting + outbox"| PGP[("Postgres Primary")]

    PGP -->|"poll outbox"| Relay["Outbox Relay"]
    Relay --> K1[["Kafka: meeting.events"]]

    K1 --> NW["Notification Worker"]
    NW -->|"email, push"| Ch["Kênh gửi"]
    NW -.->|"lỗi tạm thời"| R1[["notify.retry.5s"]]
    R1 -.->|"vẫn lỗi"| R2[["notify.retry.1m"]]
    R2 -.->|"hết lượt"| DLQ[["notify.dlq"]]

    K1 --> AI["AI Matching Worker"]
    AI -->|"gọi"| Gem["Gemini API"]
    AI --> PGP

    PGP ==>|"logical replication"| PGR[("Postgres Replica")]
    PGR --> Dash["Analytics API"]
    Dash --> UI
```

### Luồng nghiệp vụ trước, trong, sau sự kiện

```mermaid
flowchart LR
    subgraph T["TRƯỚC SỰ KIỆN"]
        A1["Đăng ký hồ sơ"] --> A2["Chuẩn hóa dữ liệu"]
        A2 --> A3["AI gợi ý ghép cặp"]
        A3 --> A4["Hai bên xác nhận quan tâm"]
        A4 --> A5["Xếp lịch 1:1"]
    end
    subgraph D["TRONG SỰ KIỆN"]
        B1["Check-in QR"] --> B2["Gặp 1:1"]
        B2 --> B3["Ghi kết quả"]
        B3 --> B4["Đổi lịch, xử lý vắng mặt"]
    end
    subgraph S["SAU SỰ KIỆN"]
        C1["Nhắc follow-up"] --> C2["Theo dõi thương vụ"]
        C2 --> C3["Dashboard hiệu quả"]
    end
    T --> D --> S
```

### Đặt lịch 1:1 (Idempotency + Redis concurrency)

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant A as API
    participant R as Redis
    participant P as Postgres
    participant O as Outbox Relay
    participant K as Kafka

    C->>A: POST /meetings (Idempotency-Key)
    A->>P: Tìm key trong idempotency_keys
    alt Key đã tồn tại
        P-->>A: Response đã lưu
        A-->>C: 200 trả kết quả cũ
    else Key mới
        A->>R: SET lock:slot:ID token NX PX 5000
        alt Lock thất bại
            A-->>C: 409 Slot đang được giữ
        else Lock thành công
            A->>P: BEGIN, kiểm tra slot trống
            A->>P: INSERT meeting, outbox_event, idempotency_key
            A->>P: COMMIT (UNIQUE slot_id là chốt chặn cuối)
            A->>R: Lua script: DEL nếu token khớp
            A-->>C: 201 Created
        end
    end
    O->>P: Poll outbox_events chưa gửi
    O->>K: Publish meeting.created
    O->>P: Đánh dấu đã gửi
```

### Retry và DLQ

```mermaid
flowchart TD
    A["Consumer nhận message"] --> B{"event_id đã xử lý?"}
    B -->|"Có"| Z["Bỏ qua, commit offset"]
    B -->|"Chưa"| C["Gửi thông báo"]
    C --> D{"Kết quả?"}
    D -->|"Thành công"| E["Ghi processed_events, commit offset"]
    D -->|"Lỗi tạm thời"| F{"Số lần retry?"}
    D -->|"Lỗi vĩnh viễn"| H
    F -->|"lần 1"| G1["notify.retry.5s"]
    F -->|"lần 2"| G2["notify.retry.1m"]
    F -->|"vượt ngưỡng"| H["notify.dlq kèm metadata lỗi"]
    G1 --> A
    G2 --> A
    H --> I["Tool replay thủ công"]
```

### Trạng thái cuộc gặp

```mermaid
stateDiagram-v2
    [*] --> Suggested: AI gợi ý
    Suggested --> Confirmed: hai bên quan tâm
    Suggested --> Rejected: một bên từ chối
    Confirmed --> Scheduled: xếp slot
    Scheduled --> CheckedIn: check-in
    Scheduled --> Cancelled: hủy / đổi lịch
    Scheduled --> NoShow: vắng quá N phút
    CheckedIn --> Completed: ghi kết quả
    Completed --> FollowUp: quan tâm cao
    FollowUp --> DealClosed
    FollowUp --> Dropped
    NoShow --> [*]
    Cancelled --> [*]
    Rejected --> [*]
    DealClosed --> [*]
    Dropped --> [*]
```

### Mô hình dữ liệu (ER)

```mermaid
erDiagram
    PARTICIPANT ||--o| STARTUP_PROFILE : has
    PARTICIPANT ||--o| INVESTOR_PROFILE : has
    STARTUP_PROFILE ||--o{ MATCH_SUGGESTION : "được gợi ý"
    INVESTOR_PROFILE ||--o{ MATCH_SUGGESTION : "được gợi ý"
    MATCH_SUGGESTION ||--o| MEETING : "thành"
    TIME_SLOT ||--o| MEETING : "chứa"
    MEETING ||--o| MEETING_OUTCOME : "có"
    MEETING ||--o| DEAL : "dẫn tới"
    MEETING ||--o{ OUTBOX_EVENT : "phát sinh"
    MEETING ||--o{ INTERACTION_LOG : "ghi"

    PARTICIPANT {
        uuid id PK
        string type
        string organization
        string country
        string language
    }
    STARTUP_PROFILE {
        uuid participant_id PK
        string sector
        string stage
        numeric funding_needed
        text traction
    }
    INVESTOR_PROFILE {
        uuid participant_id PK
        string[] sectors
        string[] stages
        numeric ticket_min
        numeric ticket_max
    }
    MATCH_SUGGESTION {
        uuid id PK
        uuid startup_id FK
        uuid investor_id FK
        numeric score
        text reason
        string status
    }
    TIME_SLOT {
        uuid id PK
        string table_name
        timestamptz starts_at
        timestamptz ends_at
    }
    MEETING {
        uuid id PK
        uuid suggestion_id FK
        uuid slot_id FK "UNIQUE"
        string status
    }
    MEETING_OUTCOME {
        uuid meeting_id PK
        string interest_level
        text notes
    }
    DEAL {
        uuid id PK
        uuid meeting_id FK
        string stage
        numeric value
    }
    OUTBOX_EVENT {
        uuid id PK
        string topic
        jsonb payload
        timestamptz sent_at
    }
    INTERACTION_LOG {
        bigint id PK
        uuid meeting_id
        string action
        timestamptz created_at "PARTITION KEY"
    }
```

---

## 4. Cấu trúc thư mục

```
davas-matching/
├── docker-compose.yml
├── README.md
├── backend/
│   ├── pyproject.toml
│   ├── alembic/                 # migrations
│   ├── app/
│   │   ├── main.py              # FastAPI entry
│   │   ├── core/                # config, db, redis, kafka clients
│   │   ├── api/                 # routers: auth, profiles, matches, meetings, analytics
│   │   ├── models/              # SQLAlchemy models
│   │   ├── schemas/             # Pydantic schemas
│   │   ├── services/            # scheduling, matching, idempotency, locking
│   │   ├── workers/
│   │   │   ├── outbox_relay.py
│   │   │   ├── notification.py
│   │   │   └── ai_matching.py
│   │   └── ai/                  # Gemini prompts, scoring
│   └── tests/                   # unit, integration, chaos
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   └── src/
│       ├── pages/               # Profile, Matches, Schedule, Dashboard, Admin
│       ├── components/
│       ├── api/                 # client + TanStack Query hooks
│       └── lib/
└── ops/
    ├── grafana/
    └── scripts/                 # seed data, chaos, replay DLQ
```

---

## 5. Chạy nhanh (local)

### Yêu cầu
Python 3.12, Node 20+, Docker Compose v2, uv. Windows nên dùng WSL2.

### Các bước
```bash
# 1. Hạ tầng (Postgres x2, Redis, Kafka, Kafka UI)
docker compose up -d

# 2. Backend
cd backend
uv sync
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --port 8000

# 3. Workers (mỗi lệnh một terminal)
uv run python -m app.workers.outbox_relay
uv run python -m app.workers.notification
uv run python -m app.workers.ai_matching

# 4. Frontend
cd ../frontend
npm install
npm run dev        # http://localhost:5173
```

### Biến môi trường (`backend/.env`)
```
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/davas
REPLICA_DATABASE_URL=postgresql+asyncpg://app:app@localhost:5433/davas
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP=localhost:9092
GEMINI_API_KEY=your_key_here
```

| Dịch vụ | Cổng |
|---|---|
| API | 8000 (docs tại `/docs`) |
| Frontend | 5173 |
| Postgres primary / replica | 5432 / 5433 |
| Redis | 6379 |
| Kafka / Kafka UI | 9092 / 8080 |

---

## 6. Hướng dẫn System Design

Mỗi bước dưới đây là một câu hỏi bạn nên tự trả lời khi thiết kế, kèm quyết định của project.

### Bước 1: Làm rõ yêu cầu

**Chức năng:** hồ sơ chuẩn hóa, gợi ý ghép cặp, xếp và đặt lịch 1:1, thông báo, ghi kết quả, dashboard KPI.

**Phi chức năng:**
| Yêu cầu | Mục tiêu đề xuất |
|---|---|
| Tính đúng đắn | 0 xung đột lịch (một slot một cuộc, một người không ở hai nơi) |
| Độ trễ đặt lịch | p99 dưới 300 ms |
| Độ sẵn sàng | Đặt lịch không phụ thuộc kênh gửi thông báo |
| Chịu lỗi | Không mất thông báo, không gửi trùng ở mức nghiệp vụ |
| Khả năng mở rộng | Thêm sự kiện, thêm worker mà không đổi kiến trúc |

### Bước 2: Ước lượng quy mô (giả định)

| Đại lượng | Giá trị |
|---|---|
| Đại biểu | 500 |
| Cuộc gặp 1:1 | 500 x 8 / 2 = 2.000 |
| Thông báo (tạo, nhắc, đổi) | khoảng 10.000 |
| Đỉnh đặt lịch | 500 người cùng mở lịch lúc mở đăng ký, vài trăm request/giây trong vài giây |
| Log tương tác mỗi sự kiện | khoảng 100.000 dòng |

Kết luận: **dữ liệu nhỏ, nhưng tranh chấp cao vào đúng thời điểm mở đăng ký**. Điểm khó là concurrency, không phải dung lượng. Partitioning và replication là để học và chuẩn bị mở rộng nhiều sự kiện nhiều năm, không phải bắt buộc ở quy mô này. Hãy nói thẳng điều này trong thuyết trình.

### Bước 3: Thiết kế API (REST)

| Method | Endpoint | Ghi chú |
|---|---|---|
| POST | `/profiles/startup`, `/profiles/investor` | Tạo/cập nhật hồ sơ |
| POST | `/admin/import` | Import Excel |
| GET | `/matches?participant_id=` | Danh sách gợi ý và lý do |
| POST | `/matches/{id}/respond` | Quan tâm / từ chối |
| GET | `/slots?available=true` | Slot còn trống |
| POST | `/meetings` | **Cần header `Idempotency-Key`** |
| PATCH | `/meetings/{id}` | Đổi lịch, hủy |
| POST | `/meetings/{id}/checkin` | Idempotent theo meeting |
| POST | `/meetings/{id}/outcome` | Ghi kết quả |
| GET | `/analytics/kpi` | Đọc từ replica |

Quy ước lỗi: `409` slot bị giữ hoặc đã đặt, `422` dữ liệu sai, `429` vượt rate limit.

### Bước 4: Đảm bảo đúng đắn khi đặt lịch (Redis concurrency)

Có **hai lớp bảo vệ**, mỗi lớp một nhiệm vụ:

| Lớp | Vai trò |
|---|---|
| Redis lock (`SET NX PX`, release bằng Lua kiểm tra token) | Giảm tranh chấp, từ chối sớm, nhanh |
| Ràng buộc DB (`UNIQUE(slot_id)`, exclusion constraint cho khoảng thời gian của một người) | **Chốt chặn đúng đắn cuối cùng** |

Lý do cần cả hai: lock có TTL và có thể hết hạn khi tiến trình bị treo (GC pause, mạng chậm). Khi đó hai request có thể cùng qua lock, và chỉ DB mới chặn được bản ghi thứ hai.

Nâng cao: dùng **fencing token** (số tăng dần) khi ghi, để request cũ bị từ chối.

Chống một người ở hai cuộc cùng lúc, dùng exclusion constraint:
```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;
ALTER TABLE meeting_participants
  ADD CONSTRAINT no_overlap
  EXCLUDE USING gist (participant_id WITH =, time_range WITH &&);
```

### Bước 5: Idempotency

| Tình huống | Cách xử lý |
|---|---|
| Client retry `POST /meetings` | Bảng `idempotency_keys(key UNIQUE, request_hash, response, created_at)`. Cùng key trả response cũ. Cùng key khác body trả `422`. |
| Kafka giao message hai lần | Consumer ghi `processed_events(event_id PK)` trong cùng transaction với tác dụng phụ |
| Check-in QR quét lại | Ràng buộc unique theo `(meeting_id, participant_id)`, dùng `INSERT ... ON CONFLICT DO NOTHING` |

### Bước 6: Gửi thông báo tin cậy (Outbox + Kafka retry)

**Vấn đề:** ghi DB rồi publish Kafka không atomic. Nếu publish lỗi sau khi commit, thông báo mất.

**Giải pháp (Transactional Outbox):**
1. Ghi `meeting` và `outbox_event` trong **cùng một transaction**.
2. Outbox Relay poll bảng, publish Kafka, đánh dấu đã gửi (dùng `FOR UPDATE SKIP LOCKED` để chạy nhiều relay).
3. Consumer phải idempotent vì Kafka giao **ít nhất một lần**.

**Retry theo bậc:**
| Topic | Độ trễ | Khi nào |
|---|---|---|
| `meeting.events` | tức thì | Luồng chính |
| `notify.retry.5s` | 5 giây | Lỗi tạm thời lần 1 |
| `notify.retry.1m` | 1 phút | Lỗi tạm thời lần 2 |
| `notify.dlq` | thủ công | Vượt ngưỡng hoặc lỗi vĩnh viễn |

Phân loại lỗi: timeout, 5xx, rate limit là **có thể retry**. Địa chỉ không hợp lệ, JSON hỏng là **không retry**, đưa thẳng DLQ để không chặn partition.

**Chọn partition key** là `slot_id` hoặc `meeting_id` để các sự kiện cùng cuộc gặp giữ đúng thứ tự.

### Bước 7: Lưu trữ và Partitioning

| Bảng | Chiến lược |
|---|---|
| `interaction_log` | Range partition theo `created_at` (theo tháng hoặc theo sự kiện) |
| `outbox_event` | Dọn bản ghi đã gửi bằng partition drop thay vì `DELETE` |
| Bảng nghiệp vụ nhỏ (`meeting`, `profile`) | Không cần partition |

Lưu ý: khóa partition phải nằm trong primary key. Dùng `DETACH PARTITION` để lưu trữ dữ liệu cũ. Tạo partition trước bằng job định kỳ.

### Bước 8: Tách đọc và ghi (Logical Replication)

- Primary phục vụ đặt lịch (ghi, giao dịch ngắn).
- Replica phục vụ dashboard và query phân tích nặng, nhờ vậy báo cáo không làm chậm hệ thống đặt lịch.
- Publication chỉ gồm bảng cần cho phân tích (meeting, outcome, deal, log).

Những điều cần hiểu khi học:
- `wal_level=logical` ở primary.
- Bảng cần PK hoặc `REPLICA IDENTITY` để replicate `UPDATE/DELETE`.
- **DDL không được replicate**: đổi schema phải áp dụng ở cả hai phía, subscriber trước.
- Replication slot giữ WAL, subscriber chết lâu sẽ làm đầy đĩa primary, nên cần giám sát lag.
- Dashboard chấp nhận dữ liệu trễ vài giây (eventual consistency).

### Bước 9: Migration không downtime (Alembic)

Dùng mẫu **expand / contract**:

| Giai đoạn | Việc làm |
|---|---|
| Expand | Thêm cột nullable, tạo index bằng `CREATE INDEX CONCURRENTLY`, code ghi cả cũ lẫn mới |
| Backfill | Cập nhật theo lô nhỏ, có thể dừng/tiếp tục |
| Switch | Đọc từ cột mới |
| Contract | Thêm ràng buộc `NOT VALID` rồi `VALIDATE`, xóa cột cũ ở bản phát hành sau |

Quy tắc: không đổi tên cột hoặc đổi kiểu trong một bước, đặt `lock_timeout` ngắn, mọi migration phải có kế hoạch rollback.

### Bước 10: Phần AI matching

Chia hai tầng để rẻ và dễ kiểm chứng:
1. **Lọc cứng bằng SQL/rule:** giai đoạn, lĩnh vực giao nhau, ticket size khớp, loại xung đột lợi ích.
2. **Xếp hạng mềm bằng Gemini:** nhận hồ sơ hai bên, trả về điểm và lý do ngắn dưới dạng JSON có schema cố định.

Lưu ý: kết quả LLM chỉ là **gợi ý**, người dùng luôn quyết định. Cache theo cặp, xử lý rate limit và lỗi bằng cùng cơ chế retry. Không đưa dữ liệu nhạy cảm không cần thiết vào prompt.

### Bước 11: Quan sát hệ thống (Observability)

| Chỉ số | Cảnh báo khi |
|---|---|
| Kafka consumer lag | Tăng liên tục |
| Tỉ lệ retry / DLQ | Vượt ngưỡng |
| Replication lag, kích thước slot | Lag lớn hoặc WAL phình |
| p99 latency `POST /meetings` | Vượt mục tiêu |
| Tỉ lệ 409 | Tăng đột biến |
| Số xung đột bị DB chặn | Khác 0 (cho thấy lock bị lọt) |

Dùng structured logging có `request_id`, `event_id`, `idempotency_key` để lần theo một cuộc gặp xuyên suốt hệ thống.

### Bước 12: Bảo mật

- Xác thực JWT, phân quyền theo vai trò (startup, investor, admin).
- Rate limit theo người dùng bằng Lua script trên Redis.
- Mã hóa/hạn chế dữ liệu liên hệ, chỉ lộ khi cả hai bên đã xác nhận quan tâm.
- Không commit `GEMINI_API_KEY`, dùng Secret Manager khi triển khai.
- Tuân thủ quy định bảo vệ dữ liệu cá nhân: có đồng ý thu thập, cho phép xóa hồ sơ.

### Bước 13: Đánh đổi (trade-off) nên nêu khi thuyết trình

| Quyết định | Lợi | Hại |
|---|---|---|
| Outbox thay vì publish trực tiếp | Không mất sự kiện | Thêm độ trễ và một tiến trình |
| Redis lock + DB constraint | Nhanh và đúng | Hai cơ chế để hiểu và vận hành |
| Kafka cho thông báo | Tách biệt, retry riêng, replay | Vận hành phức tạp hơn queue đơn giản |
| Replica cho dashboard | Không ảnh hưởng luồng đặt lịch | Dữ liệu trễ, DDL phải đồng bộ thủ công |
| Partitioning sớm | Sẵn sàng nhiều sự kiện | Quá mức cho 500 người, chỉ có lợi khi mở rộng |

---

## 7. Hai chế độ triển khai

| | Bản nộp thi | Bản học |
|---|---|---|
| Mục tiêu | Gọn, demo được, AI rõ ràng | Hiểu sâu hạ tầng |
| Hạ tầng | Cloud Run + Cloud SQL + Redis đơn giản | Docker Compose đầy đủ |
| Kafka | Có thể thay bằng task queue đơn giản | Có, kèm retry topic và DLQ |
| Replica | Không | Có, kèm logical replication |
| Trình bày | Phần "khả năng mở rộng" trong slide | Chạy thí nghiệm phá hệ thống |

Cuộc thi chấm theo giá trị giải pháp và demo, không chấm độ phức tạp hạ tầng.

---

## 8. Lộ trình thực hiện

```mermaid
flowchart LR
    S1["1. Hồ sơ + Idempotency"] --> S2["2. Redis lock + rate limit"]
    S2 --> S3["3. Outbox + Kafka retry + DLQ"]
    S3 --> S4["4. AI matching"]
    S4 --> S5["5. Frontend + Dashboard"]
    S5 --> S6["6. Migration zero-downtime"]
    S6 --> S7["7. Partitioning"]
    S7 --> S8["8. Logical replication"]
```

| Giai đoạn | Thí nghiệm phá hệ thống |
|---|---|
| Idempotency | Gửi 100 request cùng key song song, chỉ tạo đúng 1 cuộc gặp |
| Redis concurrency | Kill tiến trình đang giữ lock, mô phỏng lock hết hạn, xem DB chặn thế nào |
| Kafka retry | Gửi poison message, cho kênh gửi lỗi 30%, đo tỉ lệ thành công cuối |
| Migration | Chạy load test liên tục trong lúc migrate, đo p99 |
| Partitioning | Seed vài triệu dòng, so sánh `EXPLAIN` có và không có pruning |
| Logical replication | Dừng subscriber, quan sát WAL; đổi schema ở primary, quan sát lỗi |

Mỗi giai đoạn nên có 1 ADR ngắn (vấn đề, phương án, trade-off, lý do chọn).

---

## 9. KPI đo lường

| Nhóm | Chỉ số |
|---|---|
| Chất lượng ghép nối | Tỉ lệ cuộc gặp "quan tâm cao", điểm hài lòng hai bên |
| Hiệu quả lịch | Tỉ lệ lấp đầy slot, tỉ lệ vắng mặt, số xung đột (mục tiêu 0) |
| Kết nối sau sự kiện | Tỉ lệ follow-up trong 7 ngày, tỉ lệ chuyển bước tiếp theo |
| Thương vụ | Số thương vụ, vốn cam kết, thời gian từ lần gặp đầu đến chốt |
| Vận hành | Thời gian xếp lịch, tỉ lệ gửi thông báo thành công |

---

## 10. Câu hỏi cần làm rõ với BTC

1. Số đại biểu và số cuộc gặp 1:1 dự kiến?
2. Thời lượng mỗi cuộc, số bàn/phòng?
3. Dữ liệu hiện có ở dạng nào (Excel, form)?
4. Ai chọn ai: nhà đầu tư chọn, hai bên cùng chọn hay BTC xếp?
5. "Thành công thương vụ" được định nghĩa và ghi nhận thế nào?
6. Có yêu cầu song ngữ Việt-Anh không?

---

## 11. Tài nguyên

- AI Riser Vietnam: goo.gle/airiservietnam
- Handbook: goo.gle/airiser-handbook
- Cộng đồng Slack: #ai-riser-vietnam
