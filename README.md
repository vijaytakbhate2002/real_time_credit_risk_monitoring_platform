# CreditFlow — Event-Driven Credit Risk Analytics Platform

A local-first, event-driven data pipeline. A simulator streams loan application events into Kafka, events are stored in an SQL database, PySpark processes them in threshold-based micro-batches, and the resulting credit analytics are shown on a dashboard.

data source: https://www.kaggle.com/c/home-credit-default-risk/data?select=bureau.csv&utm_source=chatgpt.com

**Out of scope:** ML model training and inference (covered by a separate project), cloud deployment.

---

## Workflow

```text
Simulator ──► Kafka (application-events)
                        │
                        ▼
            ┌───────────────────────┐
            │   Validation Stream   │  ← dtype checks, null/default enforcement
            └──────────┬────────────┘
                       │
          ┌────────────┴─────────────┐
          ▼                          ▼
  Kafka (validated-events)   Kafka (dead-letter-events)
          │                          │
    ┌─────┴──────┐              rejected records
    │            │               (logged / alertable)
    ▼            ▼
Data Store    Compute
Stream        Stream
(→ SQL DB)    (→ PySpark ETL → Analytics → Redis → Dashboard)
```

### Stream startup sequence

1. **Validation Stream** starts first and warms up (`creditflow-validator` consumer group).
2. Once `validated-events` topic begins receiving data, **Data Store Stream** and **Compute Stream** start in parallel, each with their own consumer group.
3. All three `StreamingQuery` instances run concurrently inside PySpark's thread pool.
4. `spark.streams.awaitAnyTermination()` holds the process — if any query fails, the supervisor restarts it.

### How each stream works

1. **Simulator** reads rows from `selected_features_simulator.csv` and POSTs each `input_json` as a JSON event to the API, which publishes it to Kafka topic `application-events`.
2. **Kafka** buffers events durably; each consumer group tracks offsets independently.
3. **Validation Stream** consumes `application-events`, applies a shared `validate_batch(df)` function (dtype coercion, null checks, default value enforcement) via `foreachBatch`, and routes:
   - ✅ Valid records → `validated-events` topic
   - ❌ Invalid records → `dead-letter-events` topic
4. **Data Store Stream** (`creditflow-storage`) consumes `validated-events` and writes raw events + batch manifests to PostgreSQL.
5. **Compute Stream** (`creditflow-compute`) consumes `validated-events`, runs PySpark ETL (deduplication, transformation) followed by PySpark analytics, and pushes results to Redis.
6. **Serving layer (Redis)** holds the latest complete analytics so the dashboard never re-runs heavy queries on refresh.
7. **Dashboard** reads from Redis and displays analytics, batch status, and last refresh time.

### Kafka topics

| Topic | Producer | Consumers |
|---|---|---|
| `application-events` | Simulator | Validation Stream |
| `validated-events` | Validation Stream | Data Store Stream, Compute Stream |
| `dead-letter-events` | Validation Stream | Alerting / monitoring |

### Consumer groups

| Stream | Consumer Group |
|---|---|
| Validation Stream | `creditflow-validator` |
| Data Store Stream | `creditflow-storage` |
| Compute Stream | `creditflow-compute` |

Each group maintains its own Kafka offsets — one slow consumer never blocks another.

### Validation logic (shared)

All dtype and null/default checks live in a single reusable function:

```python
def validate_batch(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (valid_df, invalid_df)."""
    ...
```

Both the Validation Stream (`foreachBatch`) and any inline re-check inside the Compute Stream call this same function — no duplicated logic.

### Key design rules

- Offsets are committed only after the batch is safely recoverable.
- Batches are retry-safe: stable batch IDs, deterministic outputs, deduplication.
- Delivery is at-least-once with idempotent processing (not claimed as exactly-once).
- Record counts are reconciled (received = processed + rejected) before a batch is marked complete.
- Validation is a single shared function — one source of truth for all schema rules.
- Rule-based segments are labelled descriptively, not as model predictions or default risk.

### Example event (from simulator)

The simulator reads `selected_features_simulator.csv` and publishes each row as:

```json
{
  "application_id": "APP-100002",
  "input_json": {
    "AMT_CREDIT": 406597.5,
    "AMT_ANNUITY": 24700.5,
    "AMT_INCOME_TOTAL": 202500.0,
    "DEF_30_CNT_SOCIAL_CIRCLE": 2.0,
    "DEF_60_CNT_SOCIAL_CIRCLE": 2.0,
    "AMT_REQ_CREDIT_BUREAU_YEAR": 1.0,
    "EXT_SOURCE_2": 0.2629,
    "DAYS_BIRTH": -9461,
    "DAYS_EMPLOYED": -637,
    "NAME_CONTRACT_TYPE": "Cash loans",
    "CODE_GENDER": "M",
    "OCCUPATION_TYPE": "Laborers",
    "DEBT_TO_INCOME": 2.0079,
    "TOTAL_DPD_SOCIAL_CIRCLE": 4.0,
    "TOTAL_BUREAU_ENQUIRIES": 1.0
  }
}
```

### Stream config (initial)

```yaml
kafka:
  topics:
    raw:        application-events
    validated:  validated-events
    rejected:   dead-letter-events

streams:
  validation:
    consumer_group: creditflow-validator
    trigger_interval: 10 seconds
  data_store:
    consumer_group: creditflow-storage
    trigger_interval: 30 seconds
  compute:
    consumer_group: creditflow-compute
    trigger_interval: 30 seconds
```

---

## Tech stack

| Layer | Technology | Responsibility |
|---|---|---|
| Event source | Python simulator | Generate and publish application events |
| Event streaming | Apache Kafka | Durable event transport and buffering |
| Batch control | Python Kafka consumer | Accumulate events, trigger ETL by threshold or timeout |
| Data processing | **PySpark** | ETL and all analytics |
| Data storage | **PostgreSQL** (SQL) | Store data streamed from Kafka and batch metadata |
| Serving layer | Redis | Precomputed dashboard metrics (latest complete version) |
| Dashboard | *To be decided — see options below* | Display analytics and pipeline status |
| Testing | Pytest | Unit and integration tests |
| Containers | Docker Compose | Run everything locally |

SQL is used only for storage. All transformation and analytics logic lives in PySpark.

### Optional extensions (add only if a real need appears)

- Airflow: scheduled reconciliation, backfills, cleanup.
- Delta Lake / MinIO: managed lakehouse tables and object storage.
- Great Expectations: richer data-quality reports.
- Prometheus + Grafana: pipeline and service monitoring.

---

## Dashboard technology options

Dashboard tech is not decided yet. Pick one of these.

| Option | Type | Data source | Strengths | Trade-offs | Best when |
|---|---|---|---|---|---|
| **Streamlit** | Python app | Redis or PostgreSQL | Fastest to build, pure Python, good charts via Plotly/Altair | Reruns script on interaction, limited layout control | You want a working dashboard quickly |
| **Plotly Dash** | Python app | Redis or PostgreSQL | Fine-grained layout and callbacks, auto-refresh with `dcc.Interval`, more production-like | More boilerplate than Streamlit | You want more control and polish in Python |
| **Grafana** | Monitoring/BI tool | PostgreSQL (or Redis plugin) | Built for live, auto-refreshing panels, no frontend code | Less flexible for custom analytics views; Redis needs a plugin | Dashboard is mostly live metrics and pipeline health |
| **Apache Superset** | BI tool | PostgreSQL | Rich charts and filters, no frontend code | Heavy to run, needs analytics tables in SQL | You want a full BI-style experience |
| **Metabase** | BI tool | PostgreSQL | Easy setup, simple point-and-click charts | Fewer advanced visuals, needs analytics tables in SQL | You want a lightweight BI tool |

**Quick guide**

- Custom analytics reading precomputed results from Redis: **Streamlit** or **Dash**.
- Tool-based dashboards with no frontend code: **Grafana**, **Superset**, or **Metabase**. These read from SQL, so PySpark would also write analytics tables to PostgreSQL.
- Suggested default: **Streamlit**, switching to **Dash** if you need more control.

---

## Dashboard content

1. **Overview:** total applications, total and average requested amount, latest successful batch.
2. **Product analysis:** applications and requested amount by product, trends.
3. **Customer segmentation:** income bands, employment types, existing loan count distribution.
4. **Portfolio analytics:** requested exposure by segment, volume trends, rule-based segment summaries.
5. **Pipeline status:** last batch, records processed and rejected, processing duration, last refresh.

---

## Batch accounting

Track per batch: `batch_id`, `source_topic`, `record_count_received`, `record_count_processed`, `record_count_rejected`, `processing_started_at`, `processing_completed_at`, `status`, `output_location`.

---

## Build order

1. Docker Compose + simulator
2. Kafka publishing (`application-events` topic)
3. Validation Stream + dead-letter topic (`creditflow-validator`)
4. Data Store Stream → PostgreSQL (`creditflow-storage`)
5. Compute Stream → PySpark ETL + Analytics (`creditflow-compute`)
6. Serving layer (Redis)
7. Dashboard
8. Tests, failure and retry scenarios, benchmarks
9. Optional extensions

---

## Definition of done

- [ ] Simulator publishes events to Kafka (`application-events`).
- [ ] Validation Stream routes valid events to `validated-events` and rejects to `dead-letter-events`.
- [ ] `validate_batch()` covers all feature dtypes, nulls, and default value rules.
- [ ] Data Store Stream writes validated events and batch manifests to PostgreSQL.
- [ ] Compute Stream runs PySpark ETL (deduplication, transformation) on validated events.
- [ ] PySpark analytics generates application and portfolio metrics.
- [ ] Latest complete analytics are available in Redis for the dashboard.
- [ ] Dashboard shows analytics, batch status, and last refresh time.
- [ ] Failure and retry scenarios are tested for all three streams independently.
- [ ] The platform starts with documented Docker Compose commands.
