# EDA Findings

**Purpose:** Record exploratory findings from the raw extracts that motivated the warehouse design.

**Audience:** Analysts and engineers comparing raw data behaviour with implemented dbt models.

[Root README](../README.md)

> **Scope:**
 exploratory analysis of the raw MySQL/Mongo extracts, not the implemented dbt mart inventory. See `dbt/models/marts/schema_marts.yml` for the current marts.

> **Notebooks:** [`notebooks/mysql_eda.ipynb`](../notebooks/mysql_eda.ipynb) · [`notebooks/mongo_eda.ipynb`](../notebooks/mongo_eda.ipynb)
> **Data:** `data/mysql/` (9 tables) · `data/mongo/` (3 collections)
> **Last run:** 2026-11-16 · both notebooks executed clean (0 errors)

---

## 1. Dataset overview

| Store | Object | Rows | Notes |
|---|---|---:|---|
| MySQL | `loads` | 85,410 | 2022-01-01 → 2024-12-31, all `Completed` |
| MySQL | `trips` | 85,410 | 1:1 with loads (validated) |
| MySQL | `fuel_purchases` | 196,442 | 2022-01-01 → 2025-01-02 |
| MySQL | `drivers` | 150 | 124 Active / 26 Terminated |
| MySQL | `trucks` | 120 | 92 Active / 15 Maintenance / 13 Inactive |
| MySQL | `trailers` | 180 | all Active, all 53 ft |
| MySQL | `customers` | 200 | 168 Active / 32 Inactive |
| MySQL | `routes` | 58 | lanes |
| MySQL | `facilities` | 50 | |
| Mongo | `delivery_events` | 170,820 | 85,410 Pickup + 85,410 Delivery |
| Mongo | `maintenance_records` | 2,920 | |
| Mongo | `safety_incidents` | 170 | small — treat rates carefully |

**Headline totals:** revenue **$262.5M** · fuel cost **$95.6M** · 24.5M gallons · maintenance **$5.7M** / 72,230 h downtime · safety claims **$2.65M**.

---

## 2. Data quality

### What is clean
- **No nulls** in any MySQL table; no duplicate primary keys.
- Mongo business keys unique (`event_id`, `maintenance_id`, `incident_id`).
- **0 cast failures** across Mongo numeric/datetime columns (values are strings but well-formed).
- Referential coverage: **100%** of event `load_id`s and `trip_id`s resolve to MySQL; **0** loads without events.
- `trips` ↔ `loads` join is exactly 1:1 (85,410 = 85,410).

### What to watch
| Issue | Detail | Pipeline impact |
|---|---|---|
| Mongo is all-strings | Every field needs `try_cast` in staging | Already the design (`stg_mongo__*` + `rej_*`) |
| `on_time_flag` is `'True'`/`'False'` strings | Must normalise to boolean | Staging flag normalisation |
| Single `loaded_at` timestamp per collection | All docs share one load stamp (fixture load) | Freshness checks will look "instant" on fixtures |
| MPG nearly constant | mean 6.50, σ **0.58**, range 5.5–7.5 | Synthetic data — MPG KPIs have little variance |
| All loads `Completed`, all trips `Completed` | No status variety in fact tables | Status-based CDC demos need the simulator |
| Fuel extends ~2 days past loads | purchases → 2025-01-02 vs loads → 2024-12-31 | Fine; just note the window mismatch |

---

## 3. MySQL findings

### 3.1 Drivers
- 82.7% Active (124/150); 26 terminated with `termination_date` set.
- Experience: mean **13.5 y**, median 13.5, range 2–25; hires span 2012–2021.
- 25 unique home terminals; CDL class mix is available for segmentation.

### 3.2 Fleet
- Trucks: 76.7% Active; 12.5% in Maintenance, 10.8% Inactive — **~23% not earning**.
- Six makes roughly balanced (Peterbilt 24 … Kenworth 15); **100% Diesel**.
- Trailers uniform: all Active, all **53 ft** — no length segmentation possible.

### 3.3 Customers & facilities
- Types: Contract 75 · Spot 63 · Dedicated 62; 84% Active accounts.
- Annual revenue potential across book: **$537.6M** (vs actual 3-yr revenue $262.5M).
- Credit terms: 15/30/45/60 days. Top freight types: Automotive, Food/Beverage, Retail.
- Facilities skew to a handful of states (top-12 view in notebook).

### 3.4 Routes
- Distance: mean **1,392 mi**, median 1,271, range 92–3,141.
- Base rate/mile: mean **$2.20**, σ $0.37, range $1.52–$2.79.
- Distance vs rate scatter (coloured by transit days) shows longer lanes ≠ higher per-mile rate — classic taper.

### 3.5 Loads (seasonality & mix)
- **3 full years**, ~2,373 loads/month average.
- Booking mix: Dedicated 49.6% · Contract 25.2% · Spot 25.2%.
- Load type: Refrigerated 50.3% · Dry Van 49.7% (near split).
- Revenue/load: mean **$3,074**, range $126–$8,125.
- Accessorials: mean **$71.64**, charged on **62.3%** of loads.
- Monthly seasonality charts in notebook — volume and revenue move together.

### 3.6 Trips & fuel
- Distance mean **1,430 mi**; duration and idle tracked.
- **Idle share of duration ≈ 54%** — huge efficiency lever if real.
- MPG: mean/median **6.5**, σ 0.58 (synthetic tightness).
- Fuel: **$95.6M** total, avg PPG **$3.898** (range $3.15–$5.00); clear price trend over time in notebook.
- Top purchase states concentrated (chart in notebook).

### 3.7 Cross-table
- Revenue by customer type and **top-10 lanes by revenue** charted; trip↔load↔customer↔route join validated 1:1.
- Driver workload: trips/miles per active driver; experience vs MPG scatter shows **no meaningful slope** (again, synthetic MPG).
- MPG by make / model year: boxes nearly identical — no fleet-age signal in MPG.
- Correlation matrix: revenue driven by distance and surcharges; MPG almost uncorrelated with everything (low variance).

---

## 4. Mongo findings

### 4.1 Delivery events
| Metric | Value |
|---|---|
| Overall on-time (all events) | **55.67%** (95,095 on time / 75,725 late) |
| On-time — **Delivery only** | **44.61%** |
| Mean detention — Delivery | **106.6 min** (median 105) |
| Mean detention — Pickup | **76.5 min** (median 74) |
| Detention > 120 min | **36.17%** of events |

- Event mix is exactly 50/50 Pickup/Delivery.
- Day-of-week and hour-of-day charts show dispatch rhythm; monthly volume tracks the load curve.
- **Implication:** delivery-side on-time (44.6%) is the KPI that will dominate `mart_delivery_performance_daily`, `mart_customer_service_monthly` and `mart_driver_scorecard_monthly`; detention is heavy and right-skewed (max 239 min).

### 4.2 Maintenance
| Metric | Value |
|---|---|
| Records | 2,920 across 7 types (balanced ~400 each) |
| Total cost | **$5.73M** (mean **$1,963**, median $1,896) |
| Downtime | mean **24.7 h**, max 48 h |
| Cost range | $61 – $4,427 |

- Types: Inspection, Repair, Preventive, Engine, Tire, Transmission, Brake — all present in similar counts.
- Labor vs total-cost scatter and per-type boxplots in notebook feed `mart_truck_cost_monthly` (maintenance cost and operating cost per mile).
- Monthly cost series available for trend.

### 4.3 Safety incidents
| Metric | Value |
|---|---|
| Incidents | 170 |
| At-fault | 54 (31.8%) |
| Preventable | 64 (37.6%) |
| With injury | 33 (19.4%) |
| Claims total / mean | **$2.65M** / **$15,607** (max $64,246) |

- Types: DOT Violation 39 · Equipment Damage 35 · Accident 35 · Customer Complaint 34 · Moving Violation 27.
- **Preventable claims average $14.1k vs $16.5k non-preventable** — severity does not track preventability here; volume of preventable events is the lever.
- Small n → use counts and rates per million miles (as `mart_driver_scorecard_monthly` does), not raw percentages alone.

### 4.4 Cross-store
- 100% FK coverage events → loads/trips (see quality section).
- Driver-level on-time (delivery) distribution and experience scatter plotted; lane-level on-time for top-15 volume lanes charted for profitability joins.

---

## 5. Implications for the warehouse

| Area | Finding | Where it lands |
|---|---|---|
| Staging | Mongo strings cast cleanly today; keep `rej_*` for drift | `stg_mongo__*`, `rej_*` |
| On-time KPI | Delivery-only 44.6% vs all-events 55.7% — **define grain carefully** | Semantic layer / metric contract |
| Detention | Heavy tail (>120 min = 36%) | `fct_delivery_events`, `mart_delivery_performance_daily`, `mart_customer_service_monthly` |
| Fuel cost | $95.6M vs $262.5M revenue → **fuel ≈ 36% of revenue** | `mart_truck_cost_monthly`; lane profitability is not implemented as a mart yet |
| Idle | ~54% of trip duration | `mart_equipment_utilization_daily`, `mart_driver_scorecard_monthly` |
| Fleet utilisation | 23% of trucks not Active | `mart_equipment_utilization_daily` |
| Safety | Small n; prefer per-mile rates | `mart_driver_scorecard_monthly` |
| Synthetic tells | Constant MPG, all-Completed statuses, single `loaded_at` | Rely on simulator for CDC demos; don't overfit KPI tests to variance |
| Contracts | Column shapes match `DATABASE_SCHEMA.txt`; 0 cast fails | Source contracts can baseline from these files |

---

## 6. How to re-run

```bash
uv sync
make notebooks                                  # execute copies into out/notebooks
```

The target requires local `data/mysql` and `data/mongo` extracts and never modifies
the tracked notebooks. Or open in Jupyter and run all.

---

## 7. Chart index

**`mysql_eda.ipynb`** — null audit · driver status/experience/CDL · truck status/make/age · trailer type · customer type/status/revenue · facility type/state · lane distance/rate scatter · monthly load volume & revenue · seasonality bars · booking/type/weight distributions · trip distance/MPG/idle · fuel price & volume trends · PPG histogram · top fuel states · revenue by customer type · top lanes · driver workload · MPG by make/year · idle vs MPG · correlation heatmap.

**`mongo_eda.ipynb`** — cast audit · event type & on-time · on-time by event type · detention distributions · day/hour/month volume · maintenance type/cost/downtime · labor vs cost · monthly maintenance cost · safety type/at-fault/preventable/claim · severity crosstabs · driver on-time distribution · experience vs on-time · lane on-time top-15.
