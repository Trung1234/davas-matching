# davas-matching

Nền tảng **ghép nối, xếp lịch 1:1 và đo lường** cho sự kiện đầu tư khởi nghiệp DAVAS
(đề #10, AI Riser Vietnam 2026). Phần AI được thiết kế theo **Harness Engineering (taxonomy ETCLOVG)**,
kết hợp **Gemini** (sinh văn bản, agent) và **Jev** (mô hình ra quyết định có xác suất).
Phần backend đồng thời là bài học về Redis concurrency, Kafka retry, Idempotency,
PostgreSQL Partitioning, Logical Replication và Migration.

> Quy tắc nghiệp vụ, quy mô và KPI trong tài liệu là giả định, cần xác nhận với BTC/mentor.
> Các chi tiết về Jev (endpoint, field, giá) phải kiểm tra lại ở tài liệu chính thức của TypeSafe AI trước khi code.

---

## Mục lục
1. [Tổng quan](#1-tổng-quan)
2. [Nguyên tắc thiết kế](#2-nguyên-tắc-thiết-kế)
3. [Tech stack](#3-tech-stack)
4. [Kiến trúc tổng thể](#4-kiến-trúc-tổng-thể)
5. [Harness Engineering: ETCLOVG](#5-harness-engineering-etclovg)
6. [Jev: lớp quyết định](#6-jev-lớp-quyết-định)
7. [Luồng nghiệp vụ chính](#7-luồng-nghiệp-vụ-chính)
8. [Mô hình dữ liệu](#8-mô-hình-dữ-liệu)
9. [System design guide](#9-system-design-guide)
10. [Cấu trúc thư mục](#10-cấu-trúc-thư-mục)
11. [Chạy local](#11-chạy-local)
12. [Lộ trình](#12-lộ-trình)
13. [KPI](#13-kpi)
14. [Câu hỏi mở](#14-câu-hỏi-mở)

---

## 1. Tổng quan

| Thách thức trong đề | Giải pháp |
|---|---|
| Ghép nối thủ công, khó đúng khẩu vị và giai đoạn | SQL lọc cứng → **Jev** chấm điểm có xác suất → **Gemini** viết lý do |
| Lịch 1:1 quy mô lớn dễ trùng, sai đối tượng | Redis lock + ràng buộc Postgres (0 xung đột) |
| Cần theo dõi kết nối sau sự kiện | State machine cuộc gặp, follow-up tự động |
| Dữ liệu hồ sơ phân tán, chưa chuẩn hóa | Import Excel, **Jev Choice** chuẩn hóa lĩnh vực/giai đoạn |
| Chưa đo lường hiệu quả | Dashboard KPI trên replica |
| Khó cá nhân hóa lịch trình/hội thảo | Gợi ý session theo hồ sơ |
| Quy trình trước-trong-sau phân tán | Một nền tảng, nhiều vai trò, có agent hỗ trợ BTC |

**Vai trò:** Startup, Nhà đầu tư (VC/Angel), Đối tác, BTC (Admin).

---

## 2. Nguyên tắc thiết kế

1. **Code sở hữu quy trình, model chỉ điền vào các điều kiện mờ.** Thứ tự bước, retry, ghi DB do code quyết định. Model trả lời những câu "if" không viết được bằng rule.
2. **Đúng chọn đúng công cụ:**
   | Việc | Công cụ |
   |---|---|
   | Tính được chính xác (đếm, so sánh ngày, trùng lịch, ticket size) | Code / SQL / ràng buộc DB |
   | Quyết định trong tập đáp án đóng (phân loại, chấm mức, đúng/sai) | **Jev** |
   | Sinh văn bản, tóm tắt, hội thoại, gọi tool | **Gemini** |
3. **Model không bao giờ là chốt chặn đúng đắn.** Lịch không trùng là nhờ Postgres, không phải nhờ AI.
4. **Mọi quyết định AI đều có xác suất, ngưỡng và đường lui** (về người duyệt hoặc rule mặc định).
5. **Hành động ghi phải có người xác nhận** khi do agent đề xuất.

---

## 3. Tech stack

### Backend (Python 3.12)
| Mảng | Công nghệ |
|---|---|
| API | FastAPI, Uvicorn, Pydantic v2 |
| ORM / driver | SQLAlchemy 2 async, asyncpg |
| Migration | Alembic |
| Cache / lock / rate limit | Redis 7 (redis-py, Lua) |
| Messaging | Kafka (KRaft) + aiokafka |
| AI sinh văn bản / agent | Gemini (`google-genai`) |
| AI ra quyết định | Jev (TypeSafe AI) qua HTTP client `httpx` |
| Tracing AI | OpenTelemetry + Langfuse |
| Config | pydantic-settings |
| Test / eval | pytest, pytest-asyncio, httpx, Locust, bộ eval riêng |
| Package | uv |

### Frontend
| Mảng | Công nghệ |
|---|---|
| Build | Vite |
| UI | React 18 + TypeScript |
| Routing | React Router |
| Data fetching | TanStack Query |
| Form | React Hook Form + Zod |
| Style | Tailwind CSS (+ shadcn/ui) |
| Biểu đồ | Recharts |
| Lịch | FullCalendar |

### Hạ tầng
| Mảng | Công nghệ |
|---|---|
| Container | Docker Compose |
| Database | PostgreSQL 16 (primary + replica) |
| Quan sát | Prometheus, Grafana, Langfuse, Kafka UI, RedisInsight |
| Bản thi | Cloud Run, Cloud SQL, Memorystore, Secret Manager |

---

## 4. Kiến trúc tổng thể

```mermaid
flowchart LR
    UI["React + Vite"] -->|"REST"| API["FastAPI"]

    API -->|"lock, rate limit"| Redis[("Redis")]
    API -->|"meeting + outbox"| PGP[("Postgres Primary")]
    API --> AG["Organizer Agent"]

    PGP -->|"poll outbox"| Relay["Outbox Relay"]
    Relay --> K1[["Kafka: meeting.events"]]
    Relay --> K2[["Kafka: profile.events"]]

    K1 --> NW["Notification Worker"]
    NW -.->|"lỗi"| R1[["notify.retry.5s"]]
    R1 -.-> R2[["notify.retry.1m"]]
    R2 -.-> DLQ[["notify.dlq"]]

    K2 --> MW["Matching Worker"]
    MW --> JEV["Jev: Choice / Score / Noul"]
    MW --> GEM["Gemini: viết lý do"]
    AG --> GEM
    AG --> TOOLS["Tool layer"]
    TOOLS --> PGP

    MW --> PGP
    PGP ==>|"logical replication"| PGR[("Postgres Replica")]
    PGR --> AN["Analytics API"] --> UI

    API -.-> OBS["Langfuse + Prometheus"]
    MW -.-> OBS
    AG -.-> OBS
```

---

## 5. Harness Engineering: ETCLOVG

Harness là toàn bộ phần bao quanh model để model hoạt động tin cậy trong sản phẩm.
Taxonomy ETCLOVG chia harness thành 7 lớp quanh lõi **Model**.

```mermaid
flowchart TB
    O["O · Observability & Operations<br/>Tracing · Cost & latency · Failure diagnosis · Reliability"]

    subgraph CORE[" "]
        direction LR
        E["E · Execution & Sandbox<br/>Runtime · Resource boundary · Substrate"]
        T["T · Tool Interface & Protocol<br/>Schema · Protocol · Routing"]
        M["MODEL<br/>Gemini + Jev<br/>Reasoning · Decisions · Action"]
        C["C · Context & Memory<br/>Active context · Session · Long-term"]
        L["L · Lifecycle & Orchestration<br/>Agent loop · Multi-agent · Pipeline"]
    end

    V["V · Verification & Evaluation<br/>Benchmark · Readiness · Trace · Judgement · Regression"]
    G["G · Governance & Security<br/>Permission · Policy · Audit · Guardrails"]

    O --> CORE
    CORE --> V
    V --> G
```

### Model (lõi)
| Model | Vai trò | Không dùng cho |
|---|---|---|
| **Gemini** | Viết lý do ghép cặp, tóm tắt hồ sơ, agent hội thoại cho BTC, trích xuất từ Excel lộn xộn | Quyết định có tập đáp án đóng ở quy mô lớn (đắt, chậm, khó ngưỡng) |
| **Jev** | Phân loại, chấm mức tương thích, kiểm tra điều kiện, guardrail, chấm eval | Sinh text, số học, so sánh ngày, đếm |

### E · Execution Environment & Sandbox
| Thành phần | Áp dụng |
|---|---|
| Sandboxed runtime | Worker AI chạy container riêng, không có quyền DB trực tiếp ngoài tool layer |
| Resource boundary | Timeout mỗi lần gọi model (Jev 5s, Gemini 30s), giới hạn token, semaphore Redis giới hạn số call song song |
| Execution substrate | Docker Compose local, Cloud Run khi demo; worker scale theo consumer lag |

### T · Tool Interface & Protocol
Organizer Agent (Gemini function calling) chỉ thấy các tool có schema rõ ràng:

| Tool | Loại | Cần người xác nhận |
|---|---|---|
| `search_profiles(sector, stage)` | đọc | Không |
| `get_available_slots(participant_id, day)` | đọc | Không |
| `get_match_candidates(participant_id)` | đọc | Không |
| `propose_reschedule(meeting_id, slot_id)` | ghi (nháp) | **Có** |
| `send_announcement(segment, message)` | ghi | **Có** |

| Thành phần | Áp dụng |
|---|---|
| Tool schema | Pydantic model → JSON Schema cho Gemini |
| Protocol layer | Tool gọi service nội bộ qua cùng API có idempotency key, không gọi SQL thô |
| Tool routing | **Jev Choice** định tuyến yêu cầu của BTC tới nhóm tool (`schedule`, `matching`, `announcement`, `analytics`, `other`) trước khi Gemini lập kế hoạch |

### C · Context & Memory
| Thành phần | Áp dụng |
|---|---|
| Active context | Chỉ đưa vào prompt/state các trường cần cho câu hỏi (Jev giới hạn khoảng 32.000 token cho state cộng câu hỏi dài nhất); bỏ email, số điện thoại |
| Session state | Hội thoại agent lưu Redis (TTL 24h), khóa theo `session_id` |
| Long-term memory | Hồ sơ chuẩn hóa, lịch sử cuộc gặp, kết quả, phản hồi của người dùng về gợi ý trong Postgres; dùng làm dữ liệu eval |

### L · Lifecycle & Orchestration
| Thành phần | Áp dụng |
|---|---|
| Single-agent loop | Organizer Agent: lập kế hoạch → gọi tool đọc → đề xuất hành động → chờ xác nhận; tối đa N bước |
| Multi-agent coordination | Không dùng nhiều agent tự trị. Phối hợp qua Kafka giữa các worker có trách nhiệm rõ ràng |
| Task pipeline | Matching pipeline xác định: lọc SQL → Jev → top N → Gemini → lưu → thông báo (xem mục 7) |

### O · Observability & Operations
| Thành phần | Áp dụng |
|---|---|
| Tracing | Mỗi lần gọi Jev/Gemini là một span (Langfuse/OpenTelemetry) gắn `request_id`, `pair_id`, `prompt_version`, `model_version` |
| Cost & latency | Đếm token vào/ra, chi phí ước tính theo model, p50/p99 từng loại call |
| Failure diagnosis | Lưu input/output của call lỗi hoặc confidence thấp để xem lại |
| Reliability engineering | Retry có backoff, circuit breaker khi model lỗi liên tục, fallback về điểm rule-based |

### V · Verification & Evaluation
| Thành phần | Áp dụng |
|---|---|
| Benchmark grounding | Bộ **golden set** khoảng 200 cặp startup–nhà đầu tư do người gán nhãn (phù hợp / không) |
| Readiness evaluation | Chỉ bật Jev cho production khi đạt ngưỡng (ví dụ accuracy ≥ 85%, Brier ≤ 0,1 trên golden set) |
| Trace capture | Langfuse lưu toàn bộ trace để tái hiện |
| Judgement & attribution | **Jev Noul** làm judge: "Lý do Gemini viết có khớp dữ liệu hồ sơ không?" — phát hiện bịa thông tin |
| Regression feedback | Eval chạy trong CI mỗi khi đổi prompt, đổi câu hỏi Jev hoặc đổi version model; phản hồi "gợi ý này không phù hợp" của người dùng quay về golden set |

### G · Governance & Security
| Thành phần | Áp dụng |
|---|---|
| Permission control | RBAC: startup/investor chỉ xem dữ liệu của mình và gợi ý; agent chạy dưới quyền của BTC đang đăng nhập |
| Policy enforcement | Thông tin liên hệ chỉ lộ khi hai bên đã xác nhận; tool ghi luôn cần xác nhận; rate limit theo người dùng |
| Audit trail | Bảng `ai_decisions` lưu mọi quyết định AI kèm xác suất, ngưỡng, version; bảng `audit_log` cho hành động ghi |
| Security guardrails | **Jev Noul** kiểm tra prompt injection trong mô tả hồ sơ và tin nhắn gửi đi; không đưa PII không cần thiết vào model; key nằm trong Secret Manager |

---

## 6. Jev: lớp quyết định

Jev là mô hình ra quyết định của TypeSafe AI: nhận `state` và các câu hỏi có kiểu, trả về đáp án cùng xác suất, **không sinh văn bản**.

| Loại | Trả về | Giới hạn |
|---|---|---|
| **Choice** | phương án thắng, xác suất từng phương án, confidence | 2–255 phương án |
| **Score** | vị trí trên thang (có thể lẻ), xác suất từng mức, confidence | 2–10 mức có thứ tự |
| **Noul** | xác suất mệnh đề đúng, 0 đến 1 | không có confidence riêng |

Một request có thể chứa nhiều câu hỏi, Jev đánh giá song song trên cùng state.

### Các điểm quyết định trong project

| ID câu hỏi | Loại | Nội dung | Dùng khi | Ngưỡng / đường lui |
|---|---|---|---|---|
| `sector` | Choice | Lĩnh vực chính của startup (fintech, edtech, healthtech, agritech, …) | Chuẩn hóa hồ sơ import | confidence < 0,6 → BTC duyệt tay |
| `stage` | Choice | Giai đoạn (pre-seed, seed, Series A, Series B+) | Chuẩn hóa hồ sơ | confidence < 0,6 → duyệt tay |
| `thesis_fit` | Noul | "Lĩnh vực và mô hình của startup nằm trong khẩu vị đầu tư đã nêu của nhà đầu tư." | Chấm cặp | < 0,3 loại; 0,3–0,7 xem thêm Score |
| `match_level` | Score | 0 Không phù hợp · 1 Có thể · 2 Phù hợp · 3 Rất phù hợp | Xếp hạng cặp | lấy top N theo mức gần nhất |
| `conflict` | Noul | "Nhà đầu tư có dấu hiệu xung đột lợi ích với startup (đầu tư đối thủ trực tiếp)." | Chấm cặp | > 0,7 → cảnh báo BTC, không tự loại |
| `outcome` | Choice | Ghi chú sau gặp: `high_interest`, `follow_up`, `not_fit`, `unclear` | Sau cuộc gặp | `unclear` → nhắc người dùng chọn tay |
| `intent` | Choice | Định tuyến yêu cầu của BTC tới nhóm tool | Organizer Agent | `other` → Gemini xử lý tự do, chỉ tool đọc |
| `injection` | Noul | "Văn bản này chứa chỉ dẫn nhằm điều khiển hệ thống AI." | Guardrail input | > 0,5 → chặn khỏi prompt Gemini |
| `faithful` | Noul | "Mọi khẳng định trong lý do đều có căn cứ trong hai hồ sơ." | Judge output Gemini | < 0,7 → viết lại hoặc ẩn lý do |

### Những gì KHÔNG giao cho Jev
- Kiểm tra ticket size, đếm số cuộc gặp, so sánh giờ, trùng lịch: làm bằng code/SQL. Jev không đáng tin với số học, đếm và so sánh ngày.
- Viết lý do, tóm tắt: dùng Gemini.
- Diễn giải điểm Score lẻ như thước đo chính xác: chỉ lấy mức gần nhất.

### Lưu ý ngôn ngữ
Tiếng Anh là ngôn ngữ chính của Jev. Với hồ sơ tiếng Việt, viết câu hỏi và mô tả phương án bằng tiếng Anh, giữ state song ngữ nếu có, và **đo accuracy trên golden set tiếng Việt** trước khi bật.

### Client minh họa (Python)

> Tên endpoint và field dưới đây là minh họa theo cấu trúc `state` + `questions` keyed theo ID. Kiểm tra lại docs chính thức trước khi dùng.

```python
# app/ai/jev_client.py
import httpx
from app.core.config import settings

class JevClient:
    def __init__(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=settings.jev_base_url,
            headers={"Authorization": f"Bearer {settings.jev_api_key}"},
            timeout=5.0,
        )

    async def ask(self, state: dict, questions: dict) -> dict:
        resp = await self._http.post(
            "/v1/decide",
            json={"model": settings.jev_model, "state": state, "questions": questions},
        )
        resp.raise_for_status()
        return resp.json()["answers"]


MATCH_QUESTIONS = {
    "thesis_fit": {
        "type": "noul",
        "statement": "The startup's sector and business model fall within the investor's stated thesis.",
    },
    "match_level": {
        "type": "score",
        "instructions": "How well does this startup fit this investor for a 1:1 meeting?",
        "levels": [
            "Not a fit",
            "Possible fit, weak signals",
            "Good fit on sector and stage",
            "Strong fit on sector, stage and traction",
        ],
    },
    "conflict": {
        "type": "noul",
        "statement": "The investor shows signs of a conflict of interest with this startup, such as backing a direct competitor.",
    },
}
```

```python
# app/services/matching.py (rút gọn)
async def score_pair(jev, startup, investor) -> dict:
    state = {
        "startup": startup.public_fields(),    # không gồm email, SĐT
        "investor": investor.public_fields(),
    }
    a = await jev.ask(state, MATCH_QUESTIONS)
    level = round(a["match_level"]["score"])
    return {
        "thesis_fit": a["thesis_fit"]["noul"],
        "level": level,
        "confidence": a["match_level"]["confidence"],
        "conflict": a["conflict"]["noul"],
        "keep": a["thesis_fit"]["noul"] >= 0.3 and level >= 2,
    }
```

Mỗi kết quả được ghi vào `ai_decisions` (xác suất, ngưỡng, `model_version`, `question_version`) để audit và eval.

---

## 7. Luồng nghiệp vụ chính

### 7.1 Trước, trong, sau sự kiện

```mermaid
flowchart LR
    subgraph T["TRƯỚC"]
        A1["Đăng ký / import hồ sơ"] --> A2["Jev Choice chuẩn hóa"]
        A2 --> A3["Matching pipeline"]
        A3 --> A4["Hai bên xác nhận"]
        A4 --> A5["Đặt lịch 1:1"]
    end
    subgraph D["TRONG"]
        B1["Check-in QR"] --> B2["Gặp 1:1"]
        B2 --> B3["Ghi kết quả, Jev Choice phân loại"]
        B3 --> B4["Đổi lịch qua Organizer Agent"]
    end
    subgraph S["SAU"]
        C1["Follow-up tự động"] --> C2["Pipeline thương vụ"]
        C2 --> C3["Dashboard KPI"]
    end
    T --> D --> S
```

### 7.2 Matching pipeline (Jev + Gemini)

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka profile.events
    participant W as Matching Worker
    participant P as Postgres
    participant J as Jev
    participant G as Gemini
    participant L as Langfuse

    K->>W: profile.updated (event_id)
    W->>P: event_id đã xử lý? (idempotent)
    W->>P: SQL lọc cứng: stage, sector, ticket size
    loop Mỗi cặp ứng viên (song song, có semaphore)
        W->>J: state + thesis_fit, match_level, conflict
        J-->>W: xác suất + confidence
        W->>L: span quyết định
    end
    W->>W: Giữ cặp đạt ngưỡng, lấy top N
    loop Top N
        W->>G: Viết lý do từ hai hồ sơ
        G-->>W: Lý do
        W->>J: Noul faithful
        J-->>W: xác suất
    end
    W->>P: Lưu match_suggestion + ai_decisions + processed_event
```

Fallback: Jev lỗi hoặc circuit breaker mở → dùng điểm rule-based; Gemini lỗi → hiển thị gợi ý không kèm lý do.

### 7.3 Đặt lịch 1:1 (Idempotency + Redis concurrency)

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
    A->>P: Tìm key
    alt Key đã có
        A-->>C: 200 response cũ
    else Key mới
        A->>R: SET lock:slot:ID token NX PX 5000
        alt Không lấy được lock
            A-->>C: 409
        else Có lock
            A->>P: BEGIN, INSERT meeting + outbox + idempotency_key
            A->>P: COMMIT (UNIQUE slot_id, EXCLUDE trùng giờ)
            A->>R: Lua: DEL nếu token khớp
            A-->>C: 201
        end
    end
    O->>P: Poll outbox (SKIP LOCKED)
    O->>K: Publish meeting.created
```

### 7.4 Retry và DLQ

```mermaid
flowchart TD
    A["Consumer nhận message"] --> B{"event_id đã xử lý?"}
    B -->|"Có"| Z["Bỏ qua, commit"]
    B -->|"Chưa"| C["Xử lý"]
    C --> D{"Kết quả"}
    D -->|"OK"| E["Ghi processed_events, commit"]
    D -->|"Lỗi tạm thời"| F{"Lần retry"}
    D -->|"Lỗi vĩnh viễn"| H
    F -->|"1"| G1["retry.5s"]
    F -->|"2"| G2["retry.1m"]
    F -->|"vượt"| H["DLQ + metadata"]
    G1 --> A
    G2 --> A
    H --> I["Replay thủ công"]
```

### 7.5 Organizer Agent (vòng lặp đơn agent)

```mermaid
flowchart TD
    U["BTC nhập yêu cầu"] --> I1{"Jev Noul injection"}
    I1 -->|"> 0,5"| X["Từ chối"]
    I1 -->|"an toàn"| I2["Jev Choice intent"]
    I2 --> P["Gemini lập kế hoạch với nhóm tool tương ứng"]
    P --> T1["Gọi tool đọc"]
    T1 --> P
    P --> W{"Cần hành động ghi?"}
    W -->|"Không"| R["Trả lời"]
    W -->|"Có"| CF["Hiển thị đề xuất, chờ BTC xác nhận"]
    CF -->|"Đồng ý"| EX["Gọi API có Idempotency-Key"]
    CF -->|"Từ chối"| R
    EX --> AU["Ghi audit_log"] --> R
```

### 7.6 Trạng thái cuộc gặp

```mermaid
stateDiagram-v2
    [*] --> Suggested
    Suggested --> Confirmed: hai bên quan tâm
    Suggested --> Rejected
    Confirmed --> Scheduled: đặt slot
    Scheduled --> CheckedIn
    Scheduled --> Cancelled
    Scheduled --> NoShow
    CheckedIn --> Completed: ghi kết quả
    Completed --> FollowUp: high_interest
    FollowUp --> DealClosed
    FollowUp --> Dropped
    Rejected --> [*]
    Cancelled --> [*]
    NoShow --> [*]
    DealClosed --> [*]
    Dropped --> [*]
```

---

## 8. Mô hình dữ liệu

```mermaid
erDiagram
    PARTICIPANT ||--o| STARTUP_PROFILE : has
    PARTICIPANT ||--o| INVESTOR_PROFILE : has
    STARTUP_PROFILE ||--o{ MATCH_SUGGESTION : ""
    INVESTOR_PROFILE ||--o{ MATCH_SUGGESTION : ""
    MATCH_SUGGESTION ||--o{ AI_DECISION : "giải thích bởi"
    MATCH_SUGGESTION ||--o| MEETING : "thành"
    TIME_SLOT ||--o| MEETING : ""
    MEETING ||--o| MEETING_OUTCOME : ""
    MEETING ||--o| DEAL : ""
    MEETING ||--o{ OUTBOX_EVENT : ""
    MEETING ||--o{ INTERACTION_LOG : ""

    PARTICIPANT {
        uuid id PK
        string type
        string organization
        string language
    }
    STARTUP_PROFILE {
        uuid participant_id PK
        string sector
        string stage
        numeric funding_needed
        text description
    }
    INVESTOR_PROFILE {
        uuid participant_id PK
        string[] sectors
        string[] stages
        numeric ticket_min
        numeric ticket_max
        text thesis
    }
    MATCH_SUGGESTION {
        uuid id PK
        uuid startup_id FK
        uuid investor_id FK
        int level
        numeric confidence
        text reason
        string status
    }
    AI_DECISION {
        bigint id PK
        uuid subject_id
        string question_id
        string question_version
        string model_version
        jsonb answer
        numeric threshold
        timestamptz created_at "PARTITION KEY"
    }
    TIME_SLOT {
        uuid id PK
        string table_name
        tstzrange period
    }
    MEETING {
        uuid id PK
        uuid suggestion_id FK
        uuid slot_id FK "UNIQUE"
        string status
    }
    MEETING_OUTCOME {
        uuid meeting_id PK
        string label
        numeric label_prob
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

`ai_decisions` và `interaction_log` là hai bảng tăng nhanh nhất, là đối tượng thực hành partitioning.

---

## 9. System design guide

### 9.1 Yêu cầu
**Chức năng:** hồ sơ chuẩn hóa, gợi ý ghép cặp có lý do, đặt lịch 1:1, thông báo, ghi kết quả, agent hỗ trợ BTC, dashboard.

**Phi chức năng:**
| Yêu cầu | Mục tiêu |
|---|---|
| Đúng đắn lịch | 0 xung đột |
| Độ trễ đặt lịch | p99 < 300 ms |
| Độ trễ chấm một cặp (Jev) | p99 < 1 s |
| Chất lượng ghép | ≥ 85% đúng trên golden set |
| Độc lập | Đặt lịch không phụ thuộc AI hay kênh thông báo |
| Chi phí AI | Có ngân sách mỗi sự kiện, đo được theo trace |

### 9.2 Ước lượng (giả định)
| Đại lượng | Giá trị |
|---|---|
| Đại biểu | 500 (300 startup, 200 nhà đầu tư) |
| Cặp thô | 60.000 → sau lọc SQL khoảng 6.000 |
| Call Jev | khoảng 6.000 request (3 câu hỏi mỗi request) |
| Call Gemini | top 10 mỗi người → khoảng 5.000 |
| Cuộc gặp | khoảng 2.000 |

Kết luận: lọc SQL trước giảm 90% call model. Jev xử lý phần nhiều nhất vì rẻ và nhanh; Gemini chỉ chạy trên top N. Điểm khó về hạ tầng vẫn là **tranh chấp slot lúc mở đăng ký**, không phải dung lượng.

### 9.3 API
| Method | Endpoint | Ghi chú |
|---|---|---|
| POST | `/profiles/startup`, `/profiles/investor` | Phát `profile.updated` |
| POST | `/admin/import` | Import Excel, Jev chuẩn hóa |
| GET | `/matches?participant_id=` | Gợi ý, mức, lý do |
| POST | `/matches/{id}/respond` | Quan tâm / từ chối (phản hồi về eval) |
| GET | `/slots?available=true` | |
| POST | `/meetings` | **Bắt buộc `Idempotency-Key`** |
| PATCH | `/meetings/{id}` | Đổi lịch, hủy |
| POST | `/meetings/{id}/checkin` | Idempotent |
| POST | `/meetings/{id}/outcome` | Jev Choice phân loại ghi chú |
| POST | `/agent/chat` | Organizer Agent (chỉ BTC) |
| POST | `/agent/actions/{id}/confirm` | Xác nhận hành động ghi |
| GET | `/analytics/kpi` | Đọc replica |

### 9.4 Đặt lịch an toàn
| Lớp | Vai trò |
|---|---|
| Redis lock `SET NX PX` + release bằng Lua kiểm tra token | Giảm tranh chấp, từ chối sớm |
| `UNIQUE(slot_id)` + exclusion constraint | **Chốt chặn đúng đắn** |

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;
ALTER TABLE meeting_participants
  ADD CONSTRAINT no_overlap
  EXCLUDE USING gist (participant_id WITH =, period WITH &&);
```

Lock có TTL nên có thể hết hạn giữa chừng; DB vẫn chặn bản ghi thứ hai. Nâng cao: fencing token.

### 9.5 Idempotency
| Tình huống | Xử lý |
|---|---|
| Retry `POST /meetings` | `idempotency_keys(key UNIQUE, request_hash, response)`; cùng key khác body → 422 |
| Kafka giao lặp | `processed_events(event_id PK)` trong cùng transaction |
| Check-in lặp | `ON CONFLICT DO NOTHING` |
| Chấm lại cùng cặp | Cache theo `(startup_id, investor_id, profile_hash, question_version)` |

### 9.6 Outbox + Kafka retry
1. Ghi dữ liệu và `outbox_event` trong một transaction.
2. Relay poll với `FOR UPDATE SKIP LOCKED`, publish, đánh dấu.
3. Consumer idempotent (at-least-once).
4. Lỗi tạm thời (timeout, 5xx, 429 từ Jev/Gemini/kênh gửi) → retry theo bậc; lỗi vĩnh viễn → DLQ.
5. Partition key: `meeting_id` hoặc `participant_id` để giữ thứ tự.

### 9.7 Partitioning
| Bảng | Chiến lược |
|---|---|
| `ai_decisions` | Range theo `created_at` (tháng) — dữ liệu audit/eval lớn nhất |
| `interaction_log` | Range theo `created_at` |
| `outbox_event` | Drop partition cũ thay vì `DELETE` |

Khóa partition phải nằm trong PK. Tạo partition trước bằng job; `DETACH` để lưu trữ.

### 9.8 Logical replication
- Primary: đặt lịch, ghi quyết định.
- Replica: dashboard, phân tích eval (accuracy theo version, chi phí AI).
- `wal_level=logical`; bảng cần PK hoặc `REPLICA IDENTITY`.
- DDL không replicate: áp dụng ở subscriber trước.
- Giám sát lag và kích thước slot.

### 9.9 Migration (expand/contract)
| Bước | Việc |
|---|---|
| Expand | Thêm cột nullable, `CREATE INDEX CONCURRENTLY`, ghi cả cũ và mới |
| Backfill | Theo lô nhỏ, dừng/tiếp được |
| Switch | Đọc cột mới |
| Contract | Ràng buộc `NOT VALID` → `VALIDATE`, xóa cột cũ ở bản sau |

Đặt `lock_timeout` ngắn, có kế hoạch rollback. Ví dụ thực tế: thêm `question_version` vào `ai_decisions` khi đổi bộ câu hỏi Jev.

### 9.10 Versioning cho phần AI
Coi prompt Gemini và bộ câu hỏi Jev như code:
- Lưu trong `app/ai/prompts/` và `app/ai/questions/`, có version.
- Mỗi thay đổi phải chạy eval trong CI và so với baseline.
- Ghim version model (`jev-1.13.0`, không dùng `jev-latest` ở production) để kết quả tái lập được.

### 9.11 Observability
| Chỉ số | Cảnh báo khi |
|---|---|
| Consumer lag | Tăng liên tục |
| Tỉ lệ retry / DLQ | Vượt ngưỡng |
| Replication lag | Lớn hoặc WAL phình |
| p99 `POST /meetings` | > 300 ms |
| Xung đột bị DB chặn | Khác 0 |
| Tỉ lệ Jev confidence thấp | Tăng đột biến (dữ liệu lệch) |
| Tỉ lệ `faithful` < 0,7 | Tăng (Gemini bịa) |
| Chi phí AI mỗi sự kiện | Vượt ngân sách |
| Circuit breaker model | Mở |

### 9.12 Trade-off
| Quyết định | Lợi | Hại |
|---|---|---|
| Jev cho quyết định đóng | Rẻ, nhanh, có xác suất để ngưỡng | Model mới, tiếng Việt chưa đều, thêm một nhà cung cấp |
| Gemini chỉ cho top N | Giảm chi phí, giữ Gemini làm AI chính khi demo | Cặp ngoài top N không có lý do |
| Agent chỉ đọc, ghi cần xác nhận | An toàn, có audit | Chậm hơn tự động hoàn toàn |
| Pipeline xác định thay vì multi-agent | Dễ debug, dễ eval | Kém linh hoạt |
| Outbox | Không mất sự kiện | Thêm độ trễ, thêm tiến trình |
| Redis lock + DB constraint | Nhanh và đúng | Hai cơ chế cần vận hành |
| Replica cho dashboard | Không ảnh hưởng đặt lịch | Dữ liệu trễ, DDL đồng bộ thủ công |

---

## 10. Cấu trúc thư mục

```
davas-matching/
├── docker-compose.yml
├── README.md
├── docs/
│   └── adr/                     # Architecture Decision Records
├── backend/
│   ├── pyproject.toml
│   ├── alembic/
│   ├── app/
│   │   ├── main.py
│   │   ├── core/                # config, db, redis, kafka, tracing
│   │   ├── api/                 # profiles, matches, meetings, agent, analytics
│   │   ├── models/
│   │   ├── schemas/
│   │   ├── services/            # scheduling, matching, idempotency, locking
│   │   ├── ai/
│   │   │   ├── jev_client.py
│   │   │   ├── gemini_client.py
│   │   │   ├── questions/       # bộ câu hỏi Jev có version
│   │   │   ├── prompts/         # prompt Gemini có version
│   │   │   └── guardrails.py    # injection, faithful
│   │   ├── agent/
│   │   │   ├── loop.py          # single-agent loop
│   │   │   ├── tools.py         # tool schema + routing
│   │   │   └── policy.py        # quyền, xác nhận
│   │   └── workers/
│   │       ├── outbox_relay.py
│   │       ├── notification.py
│   │       └── matching.py
│   ├── evals/
│   │   ├── golden_pairs.jsonl
│   │   ├── run_eval.py          # accuracy, Brier, confusion matrix
│   │   └── thresholds.yaml
│   └── tests/                   # unit, integration, chaos
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   └── src/
│       ├── pages/               # Profile, Matches, Schedule, Dashboard, Admin, Agent
│       ├── components/
│       ├── api/
│       └── lib/
└── ops/
    ├── grafana/
    └── scripts/                 # seed, chaos, replay DLQ
```

---

## 11. Chạy local

**Yêu cầu:** Python 3.12, Node 20+, Docker Compose v2, uv. Windows dùng WSL2.

```bash
git clone https://github.com/<username>/davas-matching.git
cd davas-matching

docker compose up -d                 # Postgres x2, Redis, Kafka, Kafka UI, Langfuse

cd backend
uv sync
uv run alembic upgrade head
uv run uvicorn app.main:app --reload --port 8000

# mỗi lệnh một terminal
uv run python -m app.workers.outbox_relay
uv run python -m app.workers.notification
uv run python -m app.workers.matching

# đánh giá AI
uv run python -m evals.run_eval

cd ../frontend
npm install
npm run dev                          # http://localhost:5173
```

**`backend/.env`**
```
DATABASE_URL=postgresql+asyncpg://app:app@localhost:5432/davas
REPLICA_DATABASE_URL=postgresql+asyncpg://app:app@localhost:5433/davas
REDIS_URL=redis://localhost:6379/0
KAFKA_BOOTSTRAP=localhost:9092
GEMINI_API_KEY=
JEV_API_KEY=
JEV_BASE_URL=
JEV_MODEL=jev-1.13.0
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
AI_FALLBACK_RULES_ONLY=false         # true = tắt Jev/Gemini, chỉ dùng rule
```

| Dịch vụ | Cổng |
|---|---|
| API | 8000 (`/docs`) |
| Frontend | 5173 |
| Postgres primary / replica | 5432 / 5433 |
| Redis | 6379 |
| Kafka / Kafka UI | 9092 / 8080 |
| Langfuse | 3000 |

---

## 12. Lộ trình

```mermaid
flowchart LR
    S1["1. Hồ sơ + Idempotency"] --> S2["2. Redis lock"]
    S2 --> S3["3. Outbox + Kafka retry"]
    S3 --> S4["4. Jev + Gemini matching"]
    S4 --> S5["5. Eval + guardrails"]
    S5 --> S6["6. Organizer Agent"]
    S6 --> S7["7. Frontend + Dashboard"]
    S7 --> S8["8. Migration"]
    S8 --> S9["9. Partitioning"]
    S9 --> S10["10. Logical replication"]
```

| Giai đoạn | Thí nghiệm / tiêu chí xong |
|---|---|
| Idempotency | 100 request cùng key song song → đúng 1 cuộc gặp |
| Redis lock | Kill tiến trình giữ lock, mô phỏng lock hết hạn, DB vẫn chặn |
| Kafka retry | Poison message không chặn partition; kênh gửi lỗi 30% vẫn giao đủ |
| Matching | Jev lỗi → fallback rule-based, hệ thống không dừng |
| Eval | Golden set đạt ngưỡng; đổi câu hỏi làm giảm accuracy → CI đỏ |
| Guardrails | Hồ sơ chứa prompt injection bị chặn; lý do bịa bị ẩn |
| Agent | Mọi hành động ghi đều qua xác nhận và có audit |
| Migration | Load test liên tục khi migrate, đo p99 |
| Partitioning | `EXPLAIN` có và không có pruning trên vài triệu dòng `ai_decisions` |
| Replication | Dừng subscriber, quan sát WAL; đổi schema, quan sát lỗi |

Mỗi giai đoạn có 1 ADR trong `docs/adr/`.

### Bản thi và bản học
| | Bản nộp thi | Bản học |
|---|---|---|
| AI | Gemini là AI chính; Jev là lớp quyết định phụ (kiểm tra thể lệ) | Đầy đủ Jev + eval + guardrails |
| Hạ tầng | Cloud Run, Cloud SQL, Redis | Docker Compose đầy đủ |
| Kafka | Có thể thay bằng task queue | Retry topic + DLQ |
| Replica | Không | Có |

---

## 13. KPI

| Nhóm | Chỉ số |
|---|---|
| Ghép nối | Tỉ lệ cuộc gặp `high_interest`; tỉ lệ người dùng chấp nhận gợi ý |
| Lịch | Tỉ lệ lấp đầy slot; vắng mặt; xung đột (mục tiêu 0) |
| Sau sự kiện | Follow-up trong 7 ngày; chuyển bước thương vụ |
| Thương vụ | Số thương vụ, vốn cam kết, thời gian đến chốt |
| AI | Accuracy golden set; Brier; tỉ lệ fallback; tỉ lệ `faithful` đạt; chi phí mỗi sự kiện |
| Vận hành | p99 đặt lịch; tỉ lệ gửi thông báo thành công |

---

## 14. Câu hỏi mở

1. Số đại biểu, số cuộc gặp 1:1, thời lượng mỗi cuộc, số bàn?
2. Dữ liệu hiện có dạng gì, có mô tả khẩu vị nhà đầu tư không?
3. Ai chọn ai: nhà đầu tư chọn, hai bên cùng chọn hay BTC xếp?
4. "Thương vụ thành công" được ghi nhận thế nào?
5. Có cần song ngữ Việt-Anh?
6. Thể lệ có cho phép dùng model ngoài hệ sinh thái Google (Jev) không?

---

## Tài nguyên
- AI Riser Vietnam: goo.gle/airiservietnam · Handbook: goo.gle/airiser-handbook · Slack: #ai-riser-vietnam
- Jev: tài liệu chính thức của TypeSafe AI (docs.typesafe.ai)
- Langfuse: Jev as a judge — langfuse.com/docs/evaluation/evaluation-methods/jev-as-a-judge
