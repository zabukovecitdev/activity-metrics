# Simplification + Interview-Proofing Plan

> Read this top to bottom ONCE. Then only ever look at **"Do this next"**.

---

## 🎯 TL;DR (30 seconds)

- The system works and is well-built. The problem is **not bugs**, it's **unclear intent**.
- An interviewer will find 3 kinds of holes:
  1. **Docs that contradict the code** (instant credibility loss).
  2. **Half-finished things** (a DLQ nobody consumes, a metric nobody uses).
  3. **"Why did you build it this way?"** questions you haven't written answers to yet.
- Fix = **delete more than you add**, then write ONE short decisions doc.
- Total: ~10 small tasks, each **≤ 30–45 min**. Do one per sitting. Stop whenever.

---

## 🧭 How to use this plan

- ✅ One task = one commit. Don't batch.
- ⏱ Each task has a **timebox**. Timer runs out → commit what works, move on.
- 🚫 No new features until Phase 3 is done.
- 🏁 Each task has **Done when**. If you can't tick it, it's not done.
- 🧠 Stuck or bored? Skip to the next task in the **same phase**. Order inside a phase is flexible; phases are not.

---

## 📍 Do this next

**→ Task 1.3** (rename `EMWA` to `EWMA`). 15 minutes. Go.

Tasks 1.1 and 1.2 are done. The sink sets `DeliveryGuarantee.AT_LEAST_ONCE`, and invalid agent bodies are published to `<topic>_dlq`. Docs for both are in `docs/architecture.md` and `docs/operations.md`.

---

# Phase 1 — Stop the bleeding (≈ 1.5 h)

*Goal: nothing in the repo contradicts itself or looks unfinished.*

### 1.1 Docs say "no checkpointing", code has checkpointing  ⏱ 10 min
- **Done.** `kafka_evaluations_sink` sets `DeliveryGuarantee.AT_LEAST_ONCE`. `enable_checkpointing(1000)` is Flink state only. `docs/architecture.md` and `docs/operations.md` say so, including the limit that `docker compose restart jobmanager` does not load `flink_checkpoints`.
- **Done when:** doc and code tell the same story. ☑

### 1.2 The half-built DLQ  ⏱ 15 min
- **Done, by publishing rather than deleting.** `KafkaJsonProducer.dead_letter` sends an invalid agent body to `<topic>_dlq` (`raw_metrics_dlq`, `machines_dlq`). Nothing consumes those topics. A Kafka publish failure of a *valid* record is still logged and not dead-lettered. The runbook says how to read the topic.
- **Done when:** the DLQ is either gone or explained. ☑

### 1.3 Typo that will be spotted: `EMWA` vs `EWMA`  ⏱ 15 min
- Class `EMWA`, file `emwa.py`, notebook `emwa.ipynb`, but the detector is `EwmaDetector` and the algorithm name stored in ClickHouse is `"ewma"`.
- Exponentially Weighted Moving Average = **EWMA**. The typo says "didn't proofread".
- **Do:** rename file → `ewma.py`, class → `EWMA`, update imports, tests, notebook filename, `experiments/emwa_app.py`, docs (`grep -ri emwa`).
- **Done when:** `grep -ri emwa .` returns nothing (ignore git history) and `make test` is green. ☐

### 1.4 Dead code  ⏱ 10 min
- `MAD.is_anomaly()` and `EMWA.is_anomaly()` — only `.score()` is used by the detectors.
- `collector/main.py` → `EXCEPTIONS` counter only increments on Ctrl-C. A metric that measures "the dev pressed Ctrl-C" is noise. Delete it.
- **Done when:** removed, tests green. ☐

---

# Phase 2 — Simplify the code (≈ 4 h)

*Goal: fewer concepts to explain at a whiteboard. Each task REMOVES lines.*

### 2.1 Four layers for one calculation → two  ⏱ 45 min
- **Today:** `MadDetector` → builds `MAD` → returns `Score` → mapped to `Result`. Same for EWMA. Plus `Protocol Detector`.
- **Interview question:** *"Why does MadDetector wrap MAD? What does the wrapper add?"* Honest answer today: a different return type. That's a weak answer.
- **Do:** let `MAD` and `EWMA` classes themselves implement `name`, `min_points`, `evaluate(window) -> Result`. Delete `MadDetector`, `EwmaDetector`, and the intermediate `Score` classes (fold what you need into `Result`, or keep the numbers local).
- Keep the `Detector` Protocol (two classes use it, it's 5 lines).
- **Done when:** `detectors.py` is gone or < 25 lines, tests ported, all green. ☐

### 2.2 "Scored" blacklist → allowlist  ⏱ 30 min
- **Today:** `NOT_ANOMALY_SCORED = {battery…, memory.limit, uptime}` + `DEFAULT_DETECTORS` (1 % rule for "anything else") + `METRIC_DETECTORS`.
- **Problem:** you wrote rules for metrics that aren't scored or shown. A reader asks *"what's 'any other gauge'?"* Answer: nothing exists.
- **Do:** score **only** what's in `METRIC_DETECTORS` (cpu, memory.usage). Delete `NOT_ANOMALY_SCORED`, `DEFAULT_DETECTORS`; `is_scored` becomes `name in METRIC_DETECTORS`.
- **Bonus:** the README scoring table shrinks from 3 rows to 2.
- **Done when:** adding a scored metric = one dict entry. ☐

### 2.3 Cut battery metrics  ⏱ 20 min
- Collected, published, stored. Never scored, never on the dashboard (README admits it).
- YAGNI. Delete from `agent/service.py`, `agent/repository.py` (`read_battery`), README table, tests.
- **Done when:** agent returns 3 metrics. ☐

### 2.4 `Metric.metric_id` is `str | None`  ⏱ 45 min
- **Why it's smelly:** the agent builds a `Metric` *without* an id, the collector fills it in. A type that's "valid but incomplete" leaks into the writer, which has to re-validate the id.
- **Do:** agent returns `MetricObservation`s directly (that type already exists for the API). Delete the `Metric` → `MetricObservation` mapping in `agent/api.py`. Collector builds the full `Metric` with a real id. Now `metric_id: str` (required).
- **Also:** one modelling style. `Metric`, `MetricObservation`, `MetricsResponse`, and `Machine` are pydantic. `Evaluation` is still a dataclass, because the Flink job maps it with `row_type_info`. Interviewer: *"Why both?"* **Either** write the reason in one comment above `Evaluation`, **or** make it pydantic too (`row_type_info` already accepts either).
- **Done when:** no `Optional` id anywhere; one-line comment explains the split if it stays. ☐

### 2.5 Blocking call inside `async def`  ⏱ 15 min
- `agent/api.py` handlers are `async def`, but `psutil.cpu_percent(interval=0.1)` **blocks 100 ms on the event loop**. Two concurrent scrapes serialize.
- Classic interview catch.
- **Do:** make `service.collect_metrics` / `describe_machine` and the handlers plain `def` (FastAPI runs them in a threadpool). Nothing here is truly async.
- **Done when:** no `async def` without an `await` in `agent/`. ☐

### 2.6 Small things (batch in one commit)  ⏱ 20 min
- `collector/service.py::run`: `collect()` then `sleep(10)` → interval is 10 s **+ scrape time**, so it drifts. Either comment it ("gap between rounds, not a fixed rate") or sleep for the remainder.
- `Metric.timestamp` is already `float`. Leave it.
- Flink image runs Python 3.10, `Makefile` builds a 3.11 venv, and the code says "no newer syntax". Make the Makefile match the image.
- **Done when:** each either fixed or commented. ☐

---

# Phase 3 — The "Why?" doc (≈ 2 h)  ⭐ highest interview value

*Goal: every drill question has a 3-sentence answer written down, in your voice.*

Create `docs/decisions.md`. Format per entry: **Question → Short answer → Honest trade-off → What I'd change at scale.** Max 6 lines each. Say it **out loud first**, then type.

### The questions an interviewer WILL ask (ranked by likelihood)

| # | Question | Where it comes from | What your answer must contain |
|---|---|---|---|
| 1 | **"Why not just Prometheus + node_exporter + Alertmanager?"** | You already run Prometheus in compose, just to scrape your own services. | "Yes, that's the production answer. This is a learning project for Kafka/Flink/ClickHouse." Say it first, before they do. Then: what *would* justify this design (per-series state, custom algorithms, replay). |
| 2 | **"Why Flink for ~3 series per machine?"** | `docs/architecture.md` already admits "a plain Kafka consumer would do". | Own it. State the break-even (thousands of series, windows too big for memory, event-time at scale). |
| 3 | **"What delivery guarantee do you actually have end to end?"** | `EXACTLY_ONCE` in `main.py` vs `DeliveryGuarantee.AT_LEAST_ONCE` on the sink. | At-least-once into Kafka + **idempotent sink** (ReplacingMergeTree) = effectively-once *after merge*. Duplicates visible until merge → why `FINAL`. A new jobmanager does not resume the checkpoint volume. |
| 4 | **"Why three writer threads instead of one consumer on three topics?"** | `clickhouse_writer/main.py` | Independent offsets/batches/failures. Trade-off: 3 consumers + 3 clients, and one failure restarts all three. |
| 5 | **"Why not ClickHouse's Kafka engine and skip the writer?"** | You wrote a whole service for it. | Honest: control over validation, malformed-skip and metrics, plus learning. Admit the Kafka engine exists. |
| 6 | **"Why does `machines` go through Kafka? It changes every 5 minutes."** | Collector → Kafka → writer for a tiny table. | One path for all data; collector never touches the DB. Trade-off: more moving parts than needed. |
| 7 | **"How do you know the anomaly detection is correct?"** | Hand-tuned floors (10 pp CPU, 2 % memory); no labelled data. | Point at `experiments/emwa.ipynb`. Admit floors are tuned by eye, no precision/recall. Say what you'd do (inject synthetic spikes, measure). |
| 8 | **"Why MAD *and* EWMA?"** | Two algorithms per series, doubling rows. | Different failure modes: MAD = robust, symmetric; EWMA = tracks drift, rises only. Seeing both on the dashboard is the feature. |
| 9 | **"What if the collector dies / you run two?"** | `metric_id` minted per collector → duplicates with different ids (README admits it). | Single collector = SPOF, accepted. Fix = deterministic id `(machine_id, name, timestamp)`. |
| 10 | **"Why only 24 h retention?"** | TTL in migration 000001. | Home-lab scale; downsampling is listed as not done. |
| 11 | **"Why pull (collector scrapes) not push?"** | Agent exposes HTTP. | Agent stays dumb, no Kafka client on each machine, collector controls rate. Cost: must reach every agent. |
| 12 | **"Security?"** | Agent on `0.0.0.0`, no auth; DB password in compose. | Listed as not done. Say what you'd add first (token header on the agent, secrets via env file). |
| 13 | **"What happens to Flink state of idle series?"** | `ListState` has no TTL; a vanished machine keeps its state. | One line: add `StateTtlConfig` if machines come and go. |
| 14 | **"Why 1 s checkpoints?"** | `enable_checkpointing(1000)` | Probably too aggressive. Justify it or move to 10–30 s. Pick, then say why. |

> 🧠 **ADHD trick:** don't write all 14 at once. Write **1, 2, 3** first. Those three cover most of the risk.

### 3.1 Write `docs/decisions.md` entries 1–3  ⏱ 45 min ☐
### 3.2 Write entries 4–9  ⏱ 45 min ☐
### 3.3 Write entries 10–14 as one-liners  ⏱ 20 min ☐
### 3.4 Say each answer out loud once.  ⏱ 20 min ☐

---

# Phase 4 — Make the docs readable (≈ 1.5 h)

*Goal: someone landing on the repo understands it in 2 minutes.*

### 4.1 README is ~400 lines doing three jobs  ⏱ 45 min
- **Today:** README = overview + API reference + Kafka contracts + SQL cookbook + detector math. `docs/architecture.md` and `docs/operations.md` repeat parts of it.
- **Do:** README becomes **≤ 80 lines**:
  1. One paragraph: what it is and why (learning project).
  2. The diagram.
  3. `make up` / `make agent` / `make smoke` quick start.
  4. Screenshot of the Grafana dashboard 📸 (you have none).
  5. Links to `docs/architecture.md`, `docs/decisions.md`, `docs/operations.md`.
- **Move, don't rewrite:** API/Kafka/detector/DB detail → `docs/reference.md` (copy-paste the existing sections).
- **Done when:** README fits on about one and a half screens. ☐

### 4.2 Screenshot + short GIF  ⏱ 20 min
- Dashboard with a visible anomaly, in `docs/img/`. Reviewers open the README, look at the picture, decide.
- **Done when:** README shows the dashboard. ☐

### 4.3 Sharpen "Not doing yet"  ⏱ 15 min
- The list is good. Add: *tests for the Flink process function*, *no CI*, *no integration test beyond `make smoke`*. Each with the trigger that would make you do it.
- **Done when:** every "not doing" has a "would do when…". ☐

### 4.4 Add CI (cheapest portfolio signal)  ⏱ 30 min
- GitHub Actions: `uv sync` + `uv run pytest`. ~10 lines. Badge in README.
- **Done when:** green badge. ☐

---

# Phase 5 — Optional (only if you still have energy)

Pick at most **one**:

- **A.** Deterministic `metric_id` (`uuid5` of `machine_id|name|timestamp`) → kills the "two collectors" caveat. 30 min, removes a README paragraph.
- **B.** Test for `AnomalyDetector.process_element` with a fake context (the only untested core logic).
- **C.** A tiny evaluation script: inject synthetic spikes into a series, print precision/recall for MAD vs EWMA. **Directly answers question #7**, most impressive item here.

---

## ✅ Master checklist

**Phase 1** ☑ 1.1 doc lie · ☑ 1.2 DLQ · ☐ 1.3 EWMA rename · ☐ 1.4 dead code
**Phase 2** ☐ 2.1 collapse detectors · ☐ 2.2 allowlist · ☐ 2.3 drop battery · ☐ 2.4 metric_id type · ☐ 2.5 async/blocking · ☐ 2.6 small stuff
**Phase 3** ☐ 3.1 · ☐ 3.2 · ☐ 3.3 · ☐ 3.4
**Phase 4** ☐ 4.1 README · ☐ 4.2 screenshot · ☐ 4.3 not-doing · ☐ 4.4 CI

## 🧯 If you fall off the wagon

Minimum viable version of what's left (≈ 2 h): **1.3, 3.1, 4.1.**
1.1 and 1.2 are already done. That removes the remaining embarrassing rename and prepares the top 3 questions.

## 🚫 Things I deliberately did NOT tell you to change

- **Don't rip out Kafka/Flink/ClickHouse.** Over-engineering is the *point* of a learning project. The fix is to **say so** (Q1, Q2), not hide it.
- **Don't merge the three writers.** It's defensible. Write the defence down.
- **Don't add alerting, auth, or downsampling.** They're honestly listed as not done. Adding them is scope creep, not simplification.
