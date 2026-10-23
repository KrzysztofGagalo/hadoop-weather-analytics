-- Hive script: stations-weather analytics
-- Parameters (passed by run_hive.sh as --hivevar):
--   input_dir3  - HDFS directory with the MapReduce result (3)
--   input_dir4  - HDFS directory with the stations dictionary (4)
--   output_dir6 - HDFS directory for the final result (6), JSON Lines

USE default;


-- ============================================================
-- 1. TABLE OVER THE MAPREDUCE RESULT (3)
--    reducer output files part-*: 5 tab-separated columns
-- ============================================================
DROP TABLE IF EXISTS station_month_stats;

CREATE EXTERNAL TABLE station_month_stats (
    station_id      STRING,
    month           STRING,
    avg_temp        DOUBLE,
    max_wind_speed  DOUBLE,
    stormy_count    INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${input_dir3}';


-- ============================================================
-- 2. TABLE OVER THE STATIONS DICTIONARY (4)
--    CSV with a header line (skipped); station_name is quoted but not used
-- ============================================================
DROP TABLE IF EXISTS stations;

CREATE EXTERNAL TABLE stations (
    station_id    STRING,
    station_name  STRING,
    region        STRING,
    latitude      DOUBLE,
    longitude     DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${input_dir4}'
TBLPROPERTIES ('skip.header.line.count' = '1');


-- ============================================================
-- 3. TABLE FOR THE FINAL RESULT (6)
--    JsonSerDe writes every row as one JSON line; column names become JSON keys
-- ============================================================
DROP TABLE IF EXISTS region_month_report;

CREATE EXTERNAL TABLE region_month_report (
    region                 STRING,
    month                  STRING,
    region_month_avg_temp  DOUBLE,
    region_avg_temp        DOUBLE,
    avg_temp_deviation     DOUBLE,
    stations_ranking       ARRAY<STRUCT<station_id: STRING, rank_in_region: INT>>
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${output_dir6}';


-- ============================================================
-- 4. QUERY THAT FILLS THE RESULT TABLE
--    INSERT OVERWRITE replaces the previous result (re-runnable);
--    in Hive the WITH block must come BEFORE INSERT
-- ============================================================
WITH
-- 4a. JOIN: every station-month row gets its region (1800 rows)
joined AS (
    SELECT s.region, m.station_id, m.month, m.avg_temp
    FROM station_month_stats m
    JOIN stations s ON m.station_id = s.station_id
),
-- 4b. regional average for each month (600 rows)
region_month AS (
    SELECT region, month, AVG(avg_temp) AS region_month_avg_temp
    FROM joined
    GROUP BY region, month
),
-- 4c. regional average over all months (10 rows)
region_all AS (
    SELECT region, AVG(avg_temp) AS region_avg_temp
    FROM joined
    GROUP BY region
),
-- 4d. average of each station over all months (30 rows)
station_avg AS (
    SELECT region, station_id, AVG(avg_temp) AS station_avg_temp
    FROM joined
    GROUP BY region, station_id
),
-- 4d. station position within its region: warmest = 1 (30 rows)
station_rank AS (
    SELECT region, station_id,
           RANK() OVER (PARTITION BY region ORDER BY station_avg_temp DESC) AS rank_in_region
    FROM station_avg
),
-- 4d. ranking as an array of records, one per region (10 rows);
--     collect_list as a window function with ORDER BY keeps the array sorted by rank,
--     ROW_NUMBER() = 1 keeps a single row per region
region_ranking AS (
    SELECT region, stations_ranking
    FROM (
        SELECT region,
               collect_list(named_struct('station_id', station_id,
                                         'rank_in_region', rank_in_region))
                   OVER (PARTITION BY region ORDER BY rank_in_region, station_id
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS stations_ranking,
               ROW_NUMBER() OVER (PARTITION BY region ORDER BY rank_in_region, station_id) AS rn
        FROM station_rank
    ) ranked
    WHERE rn = 1
)
-- final assembly: 600 region + month rows
INSERT OVERWRITE TABLE region_month_report
SELECT rm.region,
       rm.month,
       rm.region_month_avg_temp,
       ra.region_avg_temp,
       rm.region_month_avg_temp - ra.region_avg_temp AS avg_temp_deviation,
       rr.stations_ranking
FROM region_month rm
JOIN region_all ra     ON rm.region = ra.region
JOIN region_ranking rr ON rm.region = rr.region;
