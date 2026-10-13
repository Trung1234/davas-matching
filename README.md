# davas-matching

Nền tảng **ghép nối, xếp lịch 1:1 và đo lường** cho sự kiện đầu tư khởi nghiệp DAVAS
(đề #10, AI Riser Vietnam 2026). Phần AI được thiết kế theo **Harness Engineering (taxonomy ETCLOVG)**:
**Rule engine** đưa ra quyết định có thể giải thích, **Gemini** lo phần ngôn ngữ (tiếng Việt), agent và chuẩn hóa dữ liệu tự do.
Phần backend đồng thời là bài học về Redis concurrency, Kafka retry, Idempotency,
PostgreSQL Partitioning, Logical Replication và Migration.

> Quy tắc nghiệp vụ, trọng số, quy mô và KPI trong tài liệu là giả định, cần xác nhận với BTC/mentor.

---

## Mục lục
1. [Tổng quan](#1-tổng-quan)
2. [Nguyên tắc thiết kế](#2-nguyên-tắc-thiết-kế)
3. [Tech stack](#3-tech-stack)
4. [Kiến trúc tổng thể](#4-kiến-trúc-tổng-thể)
5. [Harness Engineering: ETCLOVG](#5-harness-engineering-etclovg)
6. [Lớp quyết định: Rule engine](#6-lớp-quyết-định-rule-engine)
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
| Ghép nối thủ công, khó đúng khẩu vị và giai đoạn | **Rule engine** lọc cứng + chấm điểm có trọng số → **Gemini** viết lý do tiếng Việt từ mã lý do |
| Lịch 1:1 quy mô lớn dễ trùng, sai đối tượng | Redis lock + ràng buộc Postgres (0 xung đột) |
| Cần theo dõi kết nối sau sự kiện | State machine cuộc gặp, follow-up tự động |
| Dữ liệu hồ sơ phân tán, chưa chuẩn hóa | Từ điển đồng nghĩa + fuzzy match → Gemini structured output cho phần còn lại → BTC duyệt |
| Chưa đo lường hiệu quả | Dashboard KPI trên replica |
| Khó cá nhân hóa lịch trình/hội thảo | Gợi ý session theo tag hồ sơ |
| Quy trình trước-trong-sau phân tán | Một nền tảng, nhiều vai trò, có agent hỗ trợ BTC |

**Vai trò:** Startup, Nhà đầu tư (VC/Angel), Đối tác, BTC (Admin).

---

## 2. Nguyên tắc thiết kế

1. **Code sở hữu quy trình.** Thứ tự bước, retry, ghi DB do code quyết định.
2. **Cấu trúc hóa dữ liệu ngay từ đầu vào.** Form dùng danh mục chọn sẵn (lĩnh vực, giai đoạn, khu vực) thay vì ô nhập tự do. Dữ liệu càng có cấu trúc, càng ít cần model để quyết định.
3. **Đúng việc, đúng công cụ:**
   | Việc | Công cụ |
   |---|---|
   | Quyết định trên trường có cấu trúc (khớp giai đoạn, lĩnh vực, ticket size, xung đột) | **Rule engine** |
   | Tính toán chính xác (trùng lịch, đếm, so sánh giờ) | Code / SQL / ràng buộc DB |
   | Hiểu văn bản tự do tiếng Việt, viết lý do, hội thoại, gọi tool | **Gemini** (structured output khi cần quyết định) |
4. **Mọi quyết định đều giải thích được** bằng danh sách mã luật đã khớp (reason codes).
5. **Model không bao giờ là chốt chặn đúng đắn.** Lịch không trùng là nhờ Postgres.
6. **Hành động ghi do agent đề xuất phải có người xác nhận.**
7. **Lớp quyết định có interface chung (`DecisionProvider`)** để sau này có thể thử thêm provider khác mà không đổi pipeline.

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
| Rule engine | Engine tự viết: luật khai báo bằng YAML, predicate là hàm Python đăng ký sẵn (không dùng `eval`) |
| Chuẩn hóa văn bản | `rapidfuzz` (fuzzy match), `unidecode` (bỏ dấu khi so khớp) |
| AI ngôn ngữ / agent | Gemini (`google-genai`), structured output với Pydantic |
| Tracing | OpenTelemetry + Langfuse |
| Config | pydantic-settings |
| Test / eval | pytest, pytest-asyncio, hypothesis, httpx, Locust, bộ eval riêng |
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
    MW --> RE["Rule engine"]
    RULES[("rules/*.yaml")] --> RE
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
        M["MODEL + DECISION<br/>Gemini + Rule engine<br/>Reasoning · Decisions · Action"]
        C["C · Context & Memory<br/>Active context · Session · Long-term"]
        L["L · Lifecycle & Orchestration<br/>Agent loop · Multi-agent · Pipeline"]
    end

    V["V · Verification & Evaluation<br/>Benchmark · Readiness · Trace · Judgement · Regression"]
    G["G · Governance & Security<br/>Permission · Policy · Audit · Guardrails"]

    O --> CORE
    CORE --> V
    V --> G
```

### Lõi: Model và lớp quyết định
| Thành phần | Vai trò | Không dùng cho |
|---|---|---|
| **Rule engine** | Lọc cứng, chấm điểm cặp, phát hiện xung đột, tạo mã lý do | Hiểu văn bản tự do |
| **Gemini** | Chuẩn hóa văn bản tự do còn sót, viết lý do tiếng Việt, tóm tắt hồ sơ, agent cho BTC | Quyết định có thể viết thành luật; số học, trùng lịch |

### E · Execution Environment & Sandbox
| Thành phần | Áp dụng |
|---|---|
| Sandboxed runtime | Worker AI chạy container riêng, chỉ truy cập DB qua tool layer/service |
| Resource boundary | Timeout Gemini 30s, giới hạn token, semaphore Redis giới hạn số call song song, ngân sách call mỗi sự kiện |
| Execution substrate | Docker Compose local, Cloud Run khi demo; worker scale theo consumer lag |

### T · Tool Interface & Protocol
Organizer Agent (Gemini function calling) chỉ thấy các tool có schema rõ ràng:

| Tool | Loại | Cần người xác nhận |
|---|---|---|
| `search_profiles(sector, stage)` | đọc | Không |
| `get_available_slots(participant_id, day)` | đọc | Không |
| `get_match_candidates(participant_id)` | đọc | Không |
| `explain_match(suggestion_id)` | đọc (trả reason codes) | Không |
| `propose_reschedule(meeting_id, slot_id)` | ghi (nháp) | **Có** |
| `send_announcement(segment, message)` | ghi | **Có** |

| Thành phần | Áp dụng |
|---|---|
| Tool schema | Pydantic model → JSON Schema cho Gemini |
| Protocol layer | Tool gọi service nội bộ có idempotency key, không gọi SQL thô |
| Tool routing | Gemini function calling tự chọn tool; **allowlist theo vai trò** giới hạn tool được thấy. UI có nút tác vụ nhanh để BTC không phải gõ tự do |

### C · Context & Memory
| Thành phần | Áp dụng |
|---|---|
| Active context | Chỉ đưa vào prompt các trường cần thiết; bỏ email, số điện thoại |
| Session state | Hội thoại agent lưu Redis (TTL 24h), khóa theo `session_id` |
| Long-term memory | Hồ sơ chuẩn hóa, lịch sử cuộc gặp, kết quả, phản hồi về gợi ý, từ điển đồng nghĩa (tự mở rộng từ các lần BTC duyệt) |

### L · Lifecycle & Orchestration
| Thành phần | Áp dụng |
|---|---|
| Single-agent loop | Organizer Agent: lập kế hoạch → tool đọc → đề xuất → chờ xác nhận; tối đa N bước |
| Multi-agent coordination | Không dùng nhiều agent tự trị; các worker phối hợp qua Kafka |
| Task pipeline | Matching pipeline xác định: lọc cứng → chấm điểm → top N → Gemini viết lý do → kiểm tra → lưu (mục 7.2) |

### O · Observability & Operations
| Thành phần | Áp dụng |
|---|---|
| Tracing | Mỗi lần đánh giá luật và mỗi call Gemini là một span gắn `request_id`, `pair_id`, `ruleset_version`, `prompt_version` |
| Cost & latency | Rule engine gần như miễn phí; đo token và chi phí Gemini, p50/p99 |
| Failure diagnosis | Lưu input/output call lỗi; thống kê luật nào loại nhiều cặp nhất |
| Reliability engineering | Retry có backoff, circuit breaker cho Gemini; Gemini lỗi thì lý do sinh từ template |

### V · Verification & Evaluation
| Thành phần | Áp dụng |
|---|---|
| Benchmark grounding | Golden set khoảng 200 cặp do người gán nhãn (phù hợp / không) |
| Readiness evaluation | Chỉ phát hành ruleset mới khi đạt ngưỡng (ví dụ precision@10 ≥ 0,8) |
| Trace capture | Langfuse lưu trace để tái hiện |
| Judgement & attribution | **Mỗi điểm có thể truy ngược tới từng luật** (reason codes). Lý do Gemini được kiểm tra bằng luật: mọi con số, tên tổ chức trong lý do phải có trong dữ liệu đầu vào |
| Regression feedback | Unit test cho từng luật, property-based test (hypothesis), eval golden set trong CI; phản hồi "gợi ý không phù hợp" quay về golden set |

### G · Governance & Security
| Thành phần | Áp dụng |
|---|---|
| Permission control | RBAC; agent chạy dưới quyền BTC đang đăng nhập |
| Policy enforcement | Liên hệ chỉ lộ khi hai bên đã xác nhận; tool ghi cần xác nhận; rate limit |
| Audit trail | `ai_decisions` lưu điểm, reason codes, `ruleset_version`; `audit_log` cho hành động ghi |
| Security guardrails | Phòng prompt injection bằng **cấu trúc**, không dựa vào phát hiện: văn bản hồ sơ đưa vào prompt như dữ liệu trong khối phân tách, agent chỉ có tool đọc, ghi phải xác nhận; regex mẫu nghi vấn chỉ để gắn cờ cho BTC xem |

---

## 6. Lớp quyết định: Rule engine

### 6.1 Vì sao rule engine thay vì model phân loại
| Tiêu chí | Rule engine | Model phân loại |
|---|---|---|
| Ngôn ngữ | Không phụ thuộc (làm trên trường có cấu trúc) | Phụ thuộc chất lượng ngôn ngữ của model |
| Giải thích | Chính xác từng luật đã khớp | Xác suất, không rõ lý do |
| Chi phí, độ trễ | Gần 0, vài micro giây mỗi cặp | Có phí, có độ trễ mạng |
| Tái lập | Tất định | Phụ thuộc version model |
| Hạn chế | Cần dữ liệu có cấu trúc, phải tinh chỉnh trọng số thủ công | Xử lý được văn bản tự do |

Hạn chế "cần dữ liệu có cấu trúc" được giải quyết ở đầu vào (mục 6.4).

### 6.2 Bảng thay thế các điểm quyết định

| Điểm quyết định | Cách xử lý |
|---|---|
| Lĩnh vực, giai đoạn | Form chọn danh mục. Dữ liệu import: từ điển đồng nghĩa → fuzzy match → Gemini structured output (enum) → BTC duyệt |
| Khớp khẩu vị đầu tư | Luật: giai đoạn thuộc danh sách, lĩnh vực giao nhau |
| Mức tương thích (0–3) | Tổng điểm có trọng số → ngưỡng mức |
| Xung đột lợi ích | Luật: startup cùng lĩnh vực với công ty trong danh mục đầu tư mà nhà đầu tư khai báo là đối thủ trực tiếp → **cảnh báo**, không tự loại |
| Kết quả sau cuộc gặp | UI chọn nút: Quan tâm cao / Cần trao đổi thêm / Không phù hợp; ghi chú tự do chỉ lưu, không phân loại |
| Định tuyến yêu cầu agent | Gemini function calling + allowlist tool theo vai trò |
| Chống prompt injection | Phòng thủ cấu trúc (mục 5 · G) |
| Kiểm tra lý do có bịa không | Lý do sinh từ reason codes; luật hậu kiểm số liệu và tên riêng |

### 6.3 Luật khai báo

```yaml
# backend/app/rules/matching.v1.yaml
version: matching.v1
hard_filters:            # trượt một luật là loại cặp
  - id: STAGE_IN_THESIS
    predicate: stage_in_investor_stages
  - id: SECTOR_OVERLAP
    predicate: sector_overlap
  - id: TICKET_FIT
    predicate: funding_within_ticket
    params: { tolerance: 0.2 }   # cho phép lệch 20%
  - id: AVAILABILITY_OVERLAP
    predicate: has_common_free_slot

soft_scores:             # cộng điểm, tối đa 100
  - id: SECTOR_PRIMARY_MATCH
    predicate: primary_sector_equal
    weight: 30
  - id: STAGE_PREFERRED
    predicate: stage_is_investor_preferred
    weight: 20
  - id: TICKET_CENTER
    predicate: funding_near_ticket_center
    weight: 15
  - id: SAME_REGION
    predicate: region_overlap
    weight: 10
  - id: SHARED_LANGUAGE
    predicate: shared_language
    weight: 10
  - id: TRACTION_SIGNAL
    predicate: has_revenue_or_users
    weight: 15

warnings:                # không ảnh hưởng điểm, hiển thị cho BTC
  - id: POSSIBLE_CONFLICT
    predicate: competitor_in_portfolio

levels:                  # điểm tối thiểu cho mức 0..3
  - { level: 0, min: 0 }
  - { level: 1, min: 40 }
  - { level: 2, min: 60 }
  - { level: 3, min: 80 }
```

### 6.4 Engine (rút gọn)

```python
# backend/app/rules/engine.py
from dataclasses import dataclass, field
from typing import Callable
import yaml

Predicate = Callable[["Startup", "Investor", dict], bool]
PREDICATES: dict[str, Predicate] = {}

def predicate(name: str):
    def wrap(fn: Predicate) -> Predicate:
        PREDICATES[name] = fn
        return fn
    return wrap

@predicate("stage_in_investor_stages")
def _(s, i, p): return s.stage in i.stages

@predicate("sector_overlap")
def _(s, i, p): return bool(set(s.sectors) & set(i.sectors))

@predicate("funding_within_ticket")
def _(s, i, p):
    t = p.get("tolerance", 0)
    return i.ticket_min * (1 - t) <= s.funding_needed <= i.ticket_max * (1 + t)

# ... các predicate khác

@dataclass
class Decision:
    passed: bool
    score: int = 0
    level: int = 0
    reasons: list[str] = field(default_factory=list)   # mã luật đã khớp
    failed: list[str] = field(default_factory=list)    # mã luật bị trượt
    warnings: list[str] = field(default_factory=list)
    ruleset_version: str = ""

class RuleEngine:
    def __init__(self, path: str):
        with open(path, encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)
        for section in ("hard_filters", "soft_scores", "warnings"):
            for r in self.cfg.get(section, []):
                if r["predicate"] not in PREDICATES:
                    raise ValueError(f"Unknown predicate {r['predicate']}")

    def evaluate(self, s, i) -> Decision:
        d = Decision(passed=True, ruleset_version=self.cfg["version"])
        for r in self.cfg["hard_filters"]:
            if not PREDICATES[r["predicate"]](s, i, r.get("params", {})):
                d.passed = False
                d.failed.append(r["id"])
        if not d.passed:
            return d
        for r in self.cfg["soft_scores"]:
            if PREDICATES[r["predicate"]](s, i, r.get("params", {})):
                d.score += r["weight"]
                d.reasons.append(r["id"])
        for r in self.cfg.get("warnings", []):
            if PREDICATES[r["predicate"]](s, i, r.get("params", {})):
                d.warnings.append(r["id"])
        d.level = max(l["level"] for l in self.cfg["levels"] if d.score >= l["min"])
        return d
```

Luật được kiểm tra khi khởi động (predicate không tồn tại sẽ báo lỗi ngay). Ruleset có version, lưu kèm mọi quyết định.

### 6.5 Chuẩn hóa dữ liệu tự do

```mermaid
flowchart LR
    IN["Giá trị thô, ví dụ 'tai chinh so'"] --> N["Chuẩn hóa: lowercase, bỏ dấu"]
    N --> D{"Có trong từ điển đồng nghĩa?"}
    D -->|"Có"| OK["Gán nhãn chuẩn"]
    D -->|"Không"| F{"Fuzzy match ≥ 90?"}
    F -->|"Có"| OK
    F -->|"Không"| G["Gemini structured output, enum nhãn + 'unknown'"]
    G -->|"nhãn hợp lệ"| RV["Gán nhãn, đánh dấu chờ duyệt"]
    G -->|"unknown"| H["BTC duyệt tay"]
    RV --> H2["BTC xác nhận → bổ sung từ điển"]
```

Mỗi lần BTC duyệt sẽ làm từ điển giàu thêm, nên số lần phải gọi Gemini giảm dần.

```python
# Gemini chỉ được trả về một giá trị trong enum
from enum import Enum
from pydantic import BaseModel

class Sector(str, Enum):
    fintech = "fintech"; edtech = "edtech"; healthtech = "healthtech"
    agritech = "agritech"; ecommerce = "ecommerce"; unknown = "unknown"

class SectorLabel(BaseModel):
    sector: Sector
```

### 6.6 Lý do ghép cặp

1. Rule engine trả về reason codes, ví dụ `[SECTOR_PRIMARY_MATCH, STAGE_PREFERRED, TICKET_CENTER]`.
2. Mỗi mã có template tiếng Việt, ví dụ `STAGE_PREFERRED` → "Startup đang ở giai đoạn {stage}, nằm trong giai đoạn ưu tiên của quỹ."
3. Gemini chỉ **diễn đạt lại cho tự nhiên** từ template đã điền, không được thêm thông tin.
4. Hậu kiểm bằng luật: trích mọi con số và tên riêng trong câu Gemini viết; nếu có giá trị không có trong đầu vào thì dùng bản template.
5. Gemini lỗi hoặc hết ngân sách: hiển thị bản template.

---

## 7. Luồng nghiệp vụ chính

### 7.1 Trước, trong, sau sự kiện

```mermaid
flowchart LR
    subgraph T["TRƯỚC"]
        A1["Đăng ký bằng form danh mục / import Excel"] --> A2["Chuẩn hóa"]
        A2 --> A3["Matching pipeline"]
        A3 --> A4["Hai bên xác nhận"]
        A4 --> A5["Đặt lịch 1:1"]
    end
    subgraph D["TRONG"]
        B1["Check-in QR"] --> B2["Gặp 1:1"]
        B2 --> B3["Chọn kết quả bằng nút"]
        B3 --> B4["Đổi lịch qua Organizer Agent"]
    end
    subgraph S["SAU"]
        C1["Follow-up tự động"] --> C2["Pipeline thương vụ"]
        C2 --> C3["Dashboard KPI"]
    end
    T --> D --> S
```

### 7.2 Matching pipeline

```mermaid
sequenceDiagram
    autonumber
    participant K as Kafka profile.events
    participant W as Matching Worker
    participant P as Postgres
    participant R as Rule engine
    participant G as Gemini
    participant L as Langfuse

    K->>W: profile.updated (event_id)
    W->>P: event_id đã xử lý? (idempotent)
    W->>P: SQL tiền lọc theo chỉ mục: stage, sector
    loop Mỗi cặp ứng viên (trong bộ nhớ)
        W->>R: evaluate(startup, investor)
        R-->>W: passed, score, level, reasons, warnings
    end
    W->>W: Giữ level ≥ 2, lấy top N
    loop Top N
        W->>W: Điền template từ reason codes
        W->>G: Diễn đạt lại (không thêm thông tin)
        G-->>W: Câu lý do
        W->>W: Hậu kiểm số liệu, tên riêng
        W->>L: span
    end
    W->>P: Lưu match_suggestion + ai_decisions + processed_event
```

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

### 7.5 Organizer Agent

```mermaid
flowchart TD
    U["BTC chọn tác vụ nhanh hoặc nhập yêu cầu"] --> AL["Lọc tool theo vai trò (allowlist)"]
    AL --> P["Gemini lập kế hoạch, dữ liệu hồ sơ nằm trong khối phân tách"]
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
    CheckedIn --> Completed: chọn kết quả
    Completed --> FollowUp: quan tâm cao
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
    MATCH_SUGGESTION ||--|| AI_DECISION : "giải thích bởi"
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
        string region
        string[] languages
    }
    STARTUP_PROFILE {
        uuid participant_id PK
        string primary_sector
        string[] sectors
        string stage
        numeric funding_needed
        boolean has_traction
        text description
    }
    INVESTOR_PROFILE {
        uuid participant_id PK
        string[] sectors
        string[] stages
        string[] preferred_stages
        numeric ticket_min
        numeric ticket_max
        uuid[] competitor_portfolio
    }
    MATCH_SUGGESTION {
        uuid id PK
        uuid startup_id FK
        uuid investor_id FK
        int score
        int level
        text reason
        string status
    }
    AI_DECISION {
        bigint id PK
        uuid suggestion_id
        string ruleset_version
        string[] reasons
        string[] failed
        string[] warnings
        string prompt_version
        timestamptz created_at "PARTITION KEY"
    }
    SYNONYM {
        string field PK
        string raw_normalized PK
        string label
        string approved_by
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

## 9. System design guide

### 9.1 Yêu cầu
**Chức năng:** hồ sơ chuẩn hóa, gợi ý ghép cặp có lý do, đặt lịch 1:1, thông báo, ghi kết quả, agent hỗ trợ BTC, dashboard.

**Phi chức năng:**
| Yêu cầu | Mục tiêu |
|---|---|
| Đúng đắn lịch | 0 xung đột |
| Độ trễ đặt lịch | p99 < 300 ms |
| Chấm toàn bộ cặp một sự kiện | < 10 giây (rule engine trong bộ nhớ) |
| Chất lượng ghép | precision@10 ≥ 0,8 trên golden set |
| Giải thích được | 100% gợi ý có reason codes |
| Độc lập | Đặt lịch không phụ thuộc AI hay kênh thông báo |

### 9.2 Ước lượng (giả định)
| Đại lượng | Giá trị |
|---|---|
| Đại biểu | 500 (300 startup, 200 nhà đầu tư) |
| Cặp thô | 60.000 → sau hard filter khoảng 6.000 |
| Đánh giá luật | 60.000 lần, vài giây CPU |
| Call Gemini viết lý do | top 10 mỗi người → khoảng 5.000 (có fallback template) |
| Call Gemini chuẩn hóa | chỉ cho giá trị không khớp từ điển, giảm dần theo thời gian |
| Cuộc gặp | khoảng 2.000 |

Kết luận: phần quyết định gần như miễn phí, chi phí AI chủ yếu ở việc viết lý do và có thể tắt bất cứ lúc nào. Điểm khó hạ tầng vẫn là **tranh chấp slot lúc mở đăng ký**.

### 9.3 API
| Method | Endpoint | Ghi chú |
|---|---|---|
| POST | `/profiles/startup`, `/profiles/investor` | Form danh mục; phát `profile.updated` |
| POST | `/admin/import` | Import Excel, chuẩn hóa |
| GET | `/admin/review-queue` | Giá trị chờ BTC duyệt |
| POST | `/admin/synonyms` | Duyệt nhãn → bổ sung từ điển |
| GET | `/matches?participant_id=` | Gợi ý, mức, lý do, reason codes |
| POST | `/matches/{id}/respond` | Quan tâm / từ chối (về golden set) |
| GET | `/slots?available=true` | |
| POST | `/meetings` | **Bắt buộc `Idempotency-Key`** |
| PATCH | `/meetings/{id}` | Đổi lịch, hủy |
| POST | `/meetings/{id}/checkin` | Idempotent |
| POST | `/meetings/{id}/outcome` | Nhãn chọn từ danh mục |
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
| Chấm lại cùng cặp | Kết quả tất định; bỏ qua nếu `(pair, profile_hash, ruleset_version)` không đổi |

### 9.6 Outbox + Kafka retry
1. Ghi dữ liệu và `outbox_event` trong một transaction.
2. Relay poll với `FOR UPDATE SKIP LOCKED`, publish, đánh dấu.
3. Consumer idempotent (at-least-once).
4. Lỗi tạm thời (timeout, 5xx, 429 từ Gemini/kênh gửi) → retry theo bậc; lỗi vĩnh viễn → DLQ.
5. Partition key: `meeting_id` hoặc `participant_id` để giữ thứ tự.

### 9.7 Partitioning
| Bảng | Chiến lược |
|---|---|
| `ai_decisions` | Range theo `created_at` (tháng); mỗi lần đổi ruleset sinh lại hàng nghìn dòng |
| `interaction_log` | Range theo `created_at` |
| `outbox_event` | Drop partition cũ thay vì `DELETE` |

Khóa partition phải nằm trong PK. Tạo partition trước bằng job; `DETACH` để lưu trữ.

### 9.8 Logical replication
- Primary: đặt lịch, ghi quyết định.
- Replica: dashboard, so sánh chất lượng giữa các version ruleset.
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

Ví dụ thực tế: chuyển `sector` (text đơn) sang `primary_sector` + `sectors[]` khi luật cần nhiều lĩnh vực.

### 9.10 Quản lý luật như code
- Ruleset nằm trong `app/rules/*.yaml`, có version, review qua pull request.
- Mỗi luật có unit test; toàn bộ ruleset có property-based test (ví dụ: tăng `ticket_max` không bao giờ làm cặp đang đạt bị trượt `TICKET_FIT`).
- Đổi trọng số phải chạy eval golden set trong CI và so với baseline.
- Có thể chạy **shadow**: ruleset mới chấm song song, chỉ ghi log, so sánh trước khi bật.

### 9.11 Observability
| Chỉ số | Cảnh báo khi |
|---|---|
| Consumer lag | Tăng liên tục |
| Tỉ lệ retry / DLQ | Vượt ngưỡng |
| Replication lag | Lớn hoặc WAL phình |
| p99 `POST /meetings` | > 300 ms |
| Xung đột bị DB chặn | Khác 0 |
| Tỉ lệ người có < 3 gợi ý | Tăng (luật quá chặt) |
| Luật loại nhiều nhất | Thay đổi đột biến giữa version |
| Hàng chờ duyệt chuẩn hóa | Dồn ứ |
| Tỉ lệ lý do rơi về template | Tăng (Gemini lỗi hoặc bịa) |
| Chi phí Gemini mỗi sự kiện | Vượt ngân sách |

### 9.12 Trade-off
| Quyết định | Lợi | Hại |
|---|---|---|
| Rule engine thay model phân loại | Tất định, giải thích được, miễn phí, không phụ thuộc ngôn ngữ | Cần dữ liệu có cấu trúc, tinh chỉnh trọng số thủ công, khó nắm bắt sắc thái |
| Form danh mục | Dữ liệu sạch từ đầu | Người dùng mất tự do mô tả; danh mục phải được BTC duy trì |
| Gemini chỉ diễn đạt lại reason codes | Không bịa, dễ kiểm tra | Lý do bớt sâu sắc |
| Phòng injection bằng cấu trúc | Không phụ thuộc bộ phát hiện | Agent bị giới hạn quyền |
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
│   └── adr/
├── backend/
│   ├── pyproject.toml
│   ├── alembic/
│   ├── app/
│   │   ├── main.py
│   │   ├── core/                # config, db, redis, kafka, tracing
│   │   ├── api/                 # profiles, admin, matches, meetings, agent, analytics
│   │   ├── models/
│   │   ├── schemas/
│   │   ├── services/            # scheduling, matching, idempotency, locking
│   │   ├── rules/
│   │   │   ├── engine.py
│   │   │   ├── predicates.py
│   │   │   ├── matching.v1.yaml
│   │   │   └── reason_templates.vi.yaml
│   │   ├── normalize/
│   │   │   ├── synonyms.py      # từ điển + fuzzy
│   │   │   └── llm_label.py     # Gemini structured output
│   │   ├── ai/
│   │   │   ├── gemini_client.py
│   │   │   ├── prompts/         # prompt có version
│   │   │   └── reason_check.py  # hậu kiểm lý do
│   │   ├── agent/
│   │   │   ├── loop.py
│   │   │   ├── tools.py
│   │   │   └── policy.py
│   │   └── workers/
│   │       ├── outbox_relay.py
│   │       ├── notification.py
│   │       └── matching.py
│   ├── evals/
│   │   ├── golden_pairs.jsonl
│   │   ├── run_eval.py          # precision@k, recall, phân bố mức
│   │   └── thresholds.yaml
│   └── tests/                   # unit (từng luật), property, integration, chaos
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   └── src/
│       ├── pages/               # Profile, Matches, Schedule, Dashboard, Admin, Review, Agent
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

# kiểm thử luật và eval
uv run pytest tests/rules
uv run python -m evals.run_eval --ruleset app/rules/matching.v1.yaml

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
RULESET_PATH=app/rules/matching.v1.yaml
REASON_MODE=gemini                   # gemini | template
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
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
    S3 --> S4["4. Rule engine + chuẩn hóa"]
    S4 --> S5["5. Gemini lý do + eval"]
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
| Rule engine | Mọi luật có unit test; property test pass; chấm 60.000 cặp dưới 10 giây |
| Chuẩn hóa | Import Excel lộn xộn: phần lớn khớp từ điển, phần còn lại vào hàng chờ duyệt |
| Lý do + eval | Golden set đạt ngưỡng; lý do bịa số liệu bị thay bằng template |
| Agent | Mọi hành động ghi qua xác nhận và có audit |
| Migration | Đổi `sector` sang mảng khi load test chạy liên tục, đo p99 |
| Partitioning | `EXPLAIN` có và không có pruning trên vài triệu dòng `ai_decisions` |
| Replication | Dừng subscriber, quan sát WAL; đổi schema, quan sát lỗi |

Mỗi giai đoạn có 1 ADR trong `docs/adr/` (giai đoạn 4 nên có ADR "Chọn rule engine thay model phân loại").

### Bản thi và bản học
| | Bản nộp thi | Bản học |
|---|---|---|
| AI | Gemini (chuẩn hóa, lý do, agent) + rule engine giải thích được | Thêm eval CI, shadow ruleset |
| Hạ tầng | Cloud Run, Cloud SQL, Redis | Docker Compose đầy đủ |
| Kafka | Có thể thay bằng task queue | Retry topic + DLQ |
| Replica | Không | Có |

---

## 13. KPI

| Nhóm | Chỉ số |
|---|---|
| Ghép nối | Tỉ lệ cuộc gặp "quan tâm cao"; tỉ lệ chấp nhận gợi ý |
| Lịch | Lấp đầy slot; vắng mặt; xung đột (mục tiêu 0) |
| Sau sự kiện | Follow-up trong 7 ngày; chuyển bước thương vụ |
| Thương vụ | Số thương vụ, vốn cam kết, thời gian đến chốt |
| Chất lượng quyết định | precision@10 golden set; tỉ lệ người có đủ gợi ý; tỉ lệ cảnh báo xung đột được BTC xác nhận đúng |
| AI | Tỉ lệ giá trị cần Gemini chuẩn hóa; tỉ lệ lý do rơi về template; chi phí Gemini mỗi sự kiện |
| Vận hành | p99 đặt lịch; tỉ lệ gửi thông báo thành công |

---

## 14. Câu hỏi mở

1. Số đại biểu, số cuộc gặp 1:1, thời lượng mỗi cuộc, số bàn?
2. Dữ liệu hiện có dạng gì? Có danh mục lĩnh vực/giai đoạn chuẩn không?
3. Ai chọn ai: nhà đầu tư chọn, hai bên cùng chọn hay BTC xếp?
4. Trọng số nào quan trọng nhất với BTC (lĩnh vực, giai đoạn, ticket size)?
5. "Thương vụ thành công" được ghi nhận thế nào?
6. Có cần song ngữ Việt-Anh?

---

## Tài nguyên
- AI Riser Vietnam: goo.gle/airiservietnam · Handbook: goo.gle/airiser-handbook · Slack: #ai-riser-vietnam
