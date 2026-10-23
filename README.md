# Hadoop Weather Analytics

A batch data pipeline built on **Hadoop MapReduce (Streaming, Python)** and **Apache Hive**.
It turns 1,000,000 raw weather measurements from 30 stations into monthly regional climate statistics
with per-region station rankings, written to HDFS as **JSON Lines**.

Built as a project for a university Big Data batch processing course and run on a multi-node
Hadoop/Hive cluster in Docker (1 master + 2 workers).

## Pipeline

```
 (1) measurements (CSV, 1M rows, HDFS)
          │
          ▼
 (2) MapReduce — mapper.py → combiner.py → reducer.py           (Hadoop Streaming)
          │
          ▼
 (3) station × month statistics (TSV, 1,800 rows, HDFS) ───┐
                                                           ├──► (5) Hive — hive.hql ──► (6) region × month report
 (4) stations dictionary (CSV, 30 rows, HDFS) ─────────────┘          JOIN, GROUP BY,        (JSON Lines, 600 rows, HDFS)
                                                                       window functions
```

| Stage | Input | Output |
|---|---|---|
| **MapReduce** | 1,000,000 measurements | per station and month: `avg_temp`, `max_wind_speed`, `stormy_count` |
| **Hive** | MapReduce result + stations | per region and month: monthly average, overall regional average, deviation, station ranking |

## Data

Synthetic data generated with [DataFaker](https://www.datafaker.net/), provided by the course
([dataset description](https://github.com/BigDataBatchProcessing/ProjectDataSets/tree/main/Zestaw%2019%20-%20stations-weather)).
The dataset itself is **not included** in this repository.

- `datasource1` — measurements: `measurement_id, station_id, measurement_date, temperature_c, humidity_pct,
  wind_speed_kmh, precipitation_mm, condition` — 100 CSV files × 10,000 rows, 5 years of hourly data
- `datasource4` — stations: `station_id, station_name, region, latitude, longitude` — 30 stations in 10 Polish regions

## Stage 1 — MapReduce (Hadoop Streaming)

| File | Role |
|---|---|
| [`mapper.py`](mapreduce/mapper.py) | measurement → `station_id,month \t temperature,1,wind_speed,stormy` (skips header and malformed lines, drops unused columns) |
| [`combiner.py`](mapreduce/combiner.py) | partial aggregation on each worker before the shuffle |
| [`reducer.py`](mapreduce/reducer.py) | detects key changes in the sorted stream and emits one row per station-month |
| [`run_mr.sh`](mapreduce/run_mr.sh) | parameterised, re-runnable job submission |

Design decisions:

- **Sum and count instead of averages.** The value carries `temp_sum, count`; the reducer divides only at the very end.
  An average of averages would be wrong once the combiner merges partial groups.
- **Every field is combinable** (sum or max), so the combiner reuses the reducer logic without the division.
  On the full dataset it cuts the shuffle from **1,000,000 to ~179,000 records**.
- `Stormy` is encoded as `1/0`, so the storm count is a plain sum.
- The script removes the previous output directory first, so it can be run repeatedly.

## Stage 2 — Hive

[`hive.hql`](hive/hive.hql), run by [`run_hive.sh`](hive/run_hive.sh) through `beeline` with `--hivevar` parameters:

1. **External tables** over the MapReduce output (TSV) and the stations CSV (header skipped) — Hive reads the files
   in place and `DROP TABLE` never deletes the data.
2. **Output table with `JsonSerDe`** — every row is written as one JSON line; nested `ARRAY<STRUCT<...>>` for the ranking.
3. **One query split into CTEs** (`WITH`): `JOIN` → `GROUP BY region, month` → `GROUP BY region` →
   per-station averages → `RANK() OVER (PARTITION BY region ...)` → ranking collected into an ordered array
   (`collect_list` as a window function, then `ROW_NUMBER() = 1`).
4. **Re-runnable:** `DROP TABLE IF EXISTS` before every `CREATE`, `INSERT OVERWRITE` for the result.

Tez executes the whole query as a single DAG (2 map + 7 reduce vertices) in about 8 seconds.

Example output line:

```json
{"region":"Dolnośląskie","month":"2020-11","region_month_avg_temp":5.331857362973064,"region_avg_temp":6.434447268698791,"avg_temp_deviation":-1.1025899057257265,"stations_ranking":[{"station_id":"a79a0861-10de-4ef0-b9dc-c842c4841614","rank_in_region":1},{"station_id":"20ace9fc-eae0-4f03-8f0a-8ae2986db6ef","rank_in_region":2}]}
```

## Running it

Environment: the course's Docker cluster [BigData25](https://github.com/BigDataKJCourses/BigData25)
(Hadoop 3.4.1, Hive 4.1.0 on Tez). Inside the `master` container:

```bash
# load the data into HDFS
hadoop fs -put input /tmp/

# stage 1: MapReduce
bash mapreduce/run_mr.sh /tmp/input/datasource1 /project1/output_mr3

# stage 2: Hive
bash hive/run_hive.sh /project1/output_mr3 /tmp/input/datasource4 /project1/output6

# result
hadoop fs -cat /project1/output6/* | head -1 | python3 -m json.tool
```

## Testing

**Local test, no Hadoop required** — runs the MapReduce logic on a small hand-made sample
(including a malformed line), with and without the combiner, and compares the result with hand-computed values:

```bash
bash tests/test_mapreduce_local.sh
```

**Verification on the full dataset:**

- the MapReduce result on the cluster (3 reducers, 100 map tasks) matches a single-process local run
  (`mapper | sort | reducer`) row by row; averages differ only at the ~14th decimal place
  because floating-point sums depend on the order of addition;
- the final Hive output was checked against an independent from-scratch Python computation on the raw CSV files:
  all 600 rows, averages, deviations and rankings match;
- sanity checks: 1,800 rows after the JOIN (no station lost), 600 region-month groups,
  deviations summing to zero within every region;
- both scripts were run several times in a row with identical results.

## Assumptions

The task does not specify whether the station ranking should be computed per month or once per region.
This implementation ranks stations **once per region**, by their average temperature over all months,
so every month of a region carries the same ranking.

## Next steps

- Orchestrate both stages with **Apache Airflow** (clean output directories → MapReduce → Hive → fetch result).
