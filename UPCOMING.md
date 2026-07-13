# UPCOMING — Điểm yếu & Backlog cải thiện

> Tài liệu này gộp (1) các điểm yếu có thể bị khai thác khi phỏng vấn và (2) backlog
> cải thiện đã tier-hoá. Viết ra file để không mất khi compact context. Đây cũng là
> tài liệu ôn phỏng vấn.

## Trạng thái hiện tại (để tự-chứa ngữ cảnh)
- **Phase 1–5 DONE** + **gap-detection đã thêm** (trade_id tuần tự → `health_checks` + Grafana alert)
  + **REST backfill ĐÃ XONG** (service `backfiller` self-healing: quét gap ranges → `/historicalTrades?fromId=`
  → re-insert idempotent; cần `BINANCE_API_KEY`, không có thì idle). → điểm yếu #1 đã vá.
  + **Schema Registry + Avro ĐÃ XONG** (Phase 6): message value JSON→Avro, subject `trades-value`,
  compatibility BACKWARD; producer/loadgen serialize, consumer deserialize từ registry (không schema local).
  price/quantity vẫn Avro string (no float). → điểm yếu #8 (JSON thô) đã vá.
- **Single-node** (dev laptop + Ubuntu server 48GB). Kafka KRaft RF=1, ClickHouse single node.
- Pipeline: Binance WS → producer (`confluent-kafka`, acks=all + idempotence + flush) → Kafka
  (`trades`, 3 partition, key=symbol) → consumer group `clickhouse-writer` (at-least-once,
  commit sau insert) → ClickHouse (`ReplacingMergeTree`, `Decimal64(8)`) → Grafana.
- **Benchmark thật:** 100k events/s, p99 613ms, single node — nhờ scale consumer=3 (1/partition)
  + `BATCH_SIZE` 1000→10000. Bottleneck = **consumer insert path** (ClickHouse OK, 0 TOO_MANY_PARTS).
- ⚠️ Cờ vận hành: `.env` local đang `BATCH_SIZE=10000`; consumer scale bằng `--scale` (KHÔNG bền —
  cần `deploy.replicas:3` để 1 lệnh `up` ra 3 consumer). Kiểm tra `git status` xem đã commit hết chưa.

---

# PHẦN 1 — Điểm yếu có thể bị khai thác (interview attack surface)

**Nguyên tắc thủ tổng quát:** *pre-empt* — tự nói ra giới hạn TRƯỚC khi interviewer moi. Câu vàng:
> *"This is single-node by design, so no real fault tolerance, and there was a data-loss gap at
> the WebSocket edge — I've added gap detection, backfill is next. Here's exactly how I'd fix both…"*
> → Cái giết ứng viên là **claim quá tay rồi bị bóc**, không phải bản thân giới hạn.

### 🔴 1. "No data loss" nhưng mất data ở mép nguồn (WS)
- **Đòn:** *"Producer WebSocket fire-and-forget. Producer sập 30s thì trade lúc đó mất, mà còn không biết?"*
- Kafka acks/dedup chỉ bảo vệ **bên trong** pipeline. WS không replay → mất vĩnh viễn.
- **Trạng thái:** *detection* đã có (gap check theo trade_id tuần tự) **VÀ** *remediation* đã có
  (service `backfiller` self-heal qua `/historicalTrades`). → điểm yếu này **đã vá** — giờ là điểm MẠNH.
- **Thủ (nay là câu chuyện tốt):** "no loss bên trong" (Kafka acks + dedup) + "no loss ở source edge"
  (detect gap theo trade_id → backfill REST idempotent). Caveat trung thực còn lại: backfill cần API key,
  và outage rất dài (> BACKFILL_MAX_GAP) chủ động skip (tránh runaway) — xử lý có chủ đích, không im lặng.

### 🔴 2. Single-node → câu chuyện độ bền chỉ là lý thuyết
- **Đòn:** *"acks=all nhưng RF=1 — broker chết thì sao? acks=all lúc này = acks=1."*
- **Thủ:** thừa nhận single-node có chủ đích; nói đúng cái đổi ở prod: **RF=3 + min.insync=2 +
  unclean.leader.election=false**, ClickHouse ReplicatedMergeTree + Keeper. Điểm yếu thật: **chưa test HA**.

### 🟠 3. Con số benchmark có confound
- **Đòn:** *"Latency đo từ Kafka entry (bỏ đoạn ingest WS). Loadgen chạy chung máy. 100k có thật?"*
- (a) `trade_time` đóng dấu lúc vào Kafka → **bỏ qua** Binance→WS→producer→Kafka. (b) 2 loadgen +
  3 consumer + ClickHouse + Kafka **chung 16 vCPU** → trần thật cao hơn; 100k là **sàn có ngữ cảnh**.
- **Thủ:** chủ động khai caveat; nói cách đo đúng hơn (loadgen off-box, bấm giờ từ WS entry).

### 🟠 4. "Exactly-once" thực chất là at-least-once + dedup
- **Đòn:** *"Giữa insert và merge, query cộng dồn double-count → dashboard có lúc sai?"*
- **Thủ (đây là điểm MẠNH nếu trả lời gọn):** dùng chữ **"effectively-once"**; giải thích dedup
  ở merge-time + query khử-trùng cho additive metric + dup chỉ thoáng qua sau crash.
- Query đúng: count → `uniqExact((symbol,trade_id))`; sum/volume → subquery `GROUP BY (symbol,trade_id)`
  rồi mới `sum()`. **MV KHÔNG cứu được additive** (chạy trước dedup) — chỉ an toàn cho `max` (peak).

### 🟠 5. Partition key skew + trần 5 symbol
- **Đòn:** *"key=symbol, 5 symbol → tối đa 5 partition. Scale 100 coin/1000 sensor sao? 1 symbol
  chiếm 90% volume → hash dồn 1 partition → consumer đó nghẽn dù scale."*
- **Thủ:** trade-off symbol-key (giữ ordering) vs parallelism + skew; muốn scale: thêm key (nhiều
  symbol/device), composite key, hoặc đổi key (mất per-key ordering). **Chưa test skew.**

### 🟡 6. "Sao tự viết Python consumer?"
- **Đòn:** *"ClickHouse có Kafka table engine, có Kafka Connect sink — Python chậm (GIL), chính là bottleneck."*
- **Thủ:** justify muốn kiểm soát tường minh batching/offset/dedup để học+demo; thừa nhận prod
  throughput cao thì native engine / Connect sink đúng hơn. Đừng bảo vệ cứng.

### 🟡 7. Chi tiết ClickHouse
- **Đòn:** *"ORDER BY (symbol, trade_id) để dedup → query theo thời gian phải quét cả partition ngày?
  Đo latency dashboard khi bảng phình chưa? Sao peak Float64 mà price Decimal?"*
- **Thủ:** dựa day-partition pruning; thừa nhận chưa đo query cost khi data lớn; peak là display-metric
  nên Float đủ (không cascade Decimal).

### ⚫ 8. Các gap "production-quality?"
- ~~không Schema Registry (JSON thô)~~ ✅ ĐÃ VÁ (Avro + Schema Registry, BACKWARD compat).
- Còn lại: không test/CI · không monitor infra (broker/disk/OOM/lag) · không security
  (no TLS/SASL, ClickHouse no-password). → xem Backlog.

### ⚫ META — chiều sâu dưới áp lực
- Nếu interviewer đào 1 nhánh bất kỳ (rebalance protocol phá at-least-once thế nào; CAP áp vào Kafka)
  mà chỉ **thuộc đáp án** thay vì **hiểu**, sẽ lộ. Chống: với mỗi quyết định, hiểu **"tại sao KHÔNG chọn cái khác"**.

---

# PHẦN 2 — Backlog cải thiện (đã tier-hoá + đã chỉnh)

## 🔴 Tier 1 — impact cao nhất
1. ~~**REST Backfill**~~ ✅ **XONG** (service `backfiller`): `/api/v3/historicalTrades?fromId=` (raw
   trade_id, `X-MBX-APIKEY`), phân trang + rate-limit (429/418/Retry-After), idempotent re-insert,
   audit row vào `health_checks`, idle khi thiếu key. Còn có thể tô đậm khi demo: chạy `make backfill`
   sau khi cắt producer để thấy gap → healed.
2. **Tests + CI** ← **mục tiếp theo** (xương sống độ tin cậy, có thể coi ngang #1): unit `parse_message()` + gap/stall logic;
   integration produce→consume→verify; GitHub Actions `ruff` + `pytest` mỗi PR. ~2–3 ngày.

## 🟠 Tier 2
3. **Consumer-Lag Alert** (NÂNG ưu tiên — metric streaming kinh điển; nay chỉ có `make bench-lag` thủ công).
4. **Infra Monitoring** — làm **đúng cách qua Prometheus**, KHÔNG nhét vào healthcheck. Kafka & ClickHouse
   expose Prometheus native; `kafka-exporter` cho lag. **Gộp #3 + #4 làm 1 Prometheus stack** (cùng công sức)
   → được cả infra health + lag + "who watches the watcher".
5. **Dead Letter Queue** — cân nhắc **implement thật** (không chỉ nói): bad message nay bị `log.warning`
   rồi drop im lặng → gửi vào topic `trades.dlq` + alert + audit trail. ~0.5 ngày.
6. **Security: password ClickHouse ngay (5 phút)**; đầy đủ (SASL/SCRAM + TLS) thì **document**, không cần impl.

## 🟡 Tier 3 — polish
7. **Graceful Consumer Shutdown** — ⚠️ **KHÔNG phải bug mất data.** Consumer commit SAU insert →
   offset chưa commit thì restart reprocess (at-least-once). Chỉ là tối ưu **giảm reprocess**, không phải correctness.
8. **TTL cho bảng `trades`** (~10 phút) — nhưng là quyết định **deletion policy** (peak MV vẫn giữ max; query lịch sử mất data cũ).
9. **Structured Logging** (JSON, `structlog`/`python-json-logger`) cho Loki/ELK.
10. **Docker image** multi-stage + `.dockerignore` (gain khiêm tốn vì deps là wheel prebuilt).
11. **Makefile** `make test` + `make lint` (sau khi có tests).
12. **Dashboard**: panel volume/throughput, consumer-lag, annotations deploy/restart, drill-down.

## ⚫ Tier 4 — ROI-phỏng-vấn cao dù chỉ là docs → nên làm SỚM
13. **`docs/production.md` (HA)** — ReplicatedMergeTree+Keeper, Kafka RF=3+min.insync=2, topology. Vũ khí cho đòn #2.
14. **`docs/partition-strategy.md`** — trade-off symbol-key vs parallelism/skew; composite key cho 100+ symbol. Vũ khí đòn #5.

## ➕ Thiếu (bổ sung vào backlog)
- ~~**"Ai monitor healthcheck?"** dead-man switch~~ ✅ ĐÃ THÊM (in-stack): alert `health-monitor-heartbeat`
  fire khi heartbeat (`max(checked_at)` của freshness) cũ > 660s. Residual SPOF: cả-host-sập cần
  heartbeat push ra external dead-man service (Healthchecks.io/PagerDuty) — nâng cấp sau, chưa làm.
- **Data-quality validation** lúc ingest (giá âm/0/vô lý) — năng lực DE, cầu nối sang Phase 4 anomaly.
- **Runbook ops** (reset offset, recover, replay) — gộp vào docs.

---

# PHẦN 3 — Meta strategy (quan trọng nhất)
- Ưu tiên theo **ROI-phỏng-vấn / giờ**, KHÔNG theo "impact lên hệ thống":
  - **Rẻ + ROI cao (làm ngay ~1–1.5 ngày):** lag alert, HA doc, partition-key doc, ClickHouse password, TTL, graceful-shutdown.
  - **Đắt + đáng:** Tests+CI, ~~REST backfill~~ ✅, Prometheus stack.
  - **Đắt + tuỳ thời gian:** ~~Schema Registry~~ ✅ **ĐÃ LÀM full Avro migration** (Phase 6), DLQ.
- ⚠️ **Đừng để backlog thành hố scope-creep khiến mãi không đi phỏng vấn.** Portfolio có diminishing returns.
  Một project *"đủ tốt + defensible + hiểu sâu"* + **đã apply** > project *"hoàn hảo + chưa nộp đơn"*.
  Mục tiêu là **cái job**, không phải repo 10/10.
- **Kế hoạch đề xuất:** cụm rẻ-ROI-cao + Tests + Backfill (~1 tuần) → **document phần còn lại** → **bắt đầu apply**.
