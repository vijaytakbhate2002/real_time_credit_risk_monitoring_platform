# CreditFlow — Event-Driven Credit Risk Analytics Platform

A local-first, event-driven data pipeline. A simulator streams loan application events into Kafka, events are stored in an SQL database, PySpark processes them in threshold-based micro-batches, and the resulting credit analytics are shown on a dashboard.

data source: https://www.kaggle.com/c/home-credit-default-risk/data?select=bureau.csv&utm_source=chatgpt.com

**Out of scope:** ML model training and inference (covered by a separate project), cloud deployment.

---

## Workflow

```text
Simulator ──► Kafka ──► Batch Controller ──► SQL DB (raw events)
                              │
                              ▼
                        PySpark ETL ──► PySpark Analytics ──► Serving layer ──► Dashboard
```

1. **Simulator** generates loan application events as JSON and publishes them to a Kafka topic (`application-events`).
2. **Kafka** buffers the events durably and lets consumers resume from committed offsets.
3. **Batch controller** (Kafka consumer) accumulates events and triggers processing when either:
   - the record threshold is reached (e.g. 100 events), or
   - the max wait time expires (so partial batches are not stuck).
   Each batch gets a stable `batch_id`, and a batch manifest is recorded before processing.
4. **SQL database** stores the data streamed from Kafka (raw events and batch metadata).
5. **PySpark ETL** reads a batch, then cleans, validates, deduplicates, and transforms it. Invalid records go to a rejected output.
6. **PySpark analytics** computes application, customer, and portfolio metrics from the cleaned data.
7. **Serving layer** holds the latest complete analytics so the dashboard never re-runs heavy processing on refresh.
8. **Dashboard** displays the analytics, plus batch status and last refresh time.

### Key design rules

- Offsets are committed only after the batch is safely recoverable.
- Batches are retry-safe: stable batch IDs, deterministic outputs, deduplication.
- Delivery is at-least-once with idempotent processing (not claimed as exactly-once).
- Record counts are reconciled (received = processed + rejected) before a batch is marked complete.
- Rule-based segments are labelled descriptively, not as model predictions or default risk.

### Example event

```json
{
  "application_id": "APP-100001",
  "customer_id": "CUST-501",
  "application_timestamp": "2026-11-22T10:15:00Z",
  "loan_amount": 250000,
  "loan_term_months": 24,
  "annual_income": 850000,
  "employment_type": "Salaried",
  "existing_loan_count": 1,
  "requested_product": "Personal Loan"
}
```

Event envelope fields: `event_id`, `application_id`, `customer_id`, `event_type`, `event_timestamp`, `schema_version`, `payload`.

### Batch control config (initial)

```yaml
batch_control:
  max_records: 100
  max_wait_seconds: 30
  topic: application-events
  consumer_group: creditflow-etl
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
2. Kafka publishing
3. Batch controller (threshold + timeout)
4. PostgreSQL storage of streamed events
5. PySpark ETL
6. PySpark analytics
7. Serving layer (Redis)
8. Dashboard
9. Tests, failure and retry scenarios, benchmarks
10. Optional extensions

---

## Definition of done

- [ ] Simulator publishes events to Kafka.
- [ ] Batch controller triggers at the threshold or timeout.
- [ ] Streamed data is stored in PostgreSQL.
- [ ] PySpark ETL handles duplicate and invalid records explicitly.
- [ ] PySpark analytics generates application and portfolio metrics.
- [ ] Latest complete analytics are available to the dashboard.
- [ ] Dashboard shows analytics, batch status, and last refresh time.
- [ ] Failure and retry scenarios are tested.
- [ ] The platform starts with documented Docker Compose commands.
