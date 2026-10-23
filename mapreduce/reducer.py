#!/usr/bin/env python3
# Reducer: input lines arrive sorted by key, so all lines of one group are adjacent.
# Hadoop Streaming does not hand over ready-made groups - the reducer detects a key change itself.
# Output (5 tab-separated columns): station_id, month, avg_temp, max_wind_speed, stormy_count
import sys

current_key = None
temperature = 0.0
count = 0
max_wind = 0.0
stormy = 0


def emit(key, temperature, count, max_wind, stormy):
    station_id, month = key.split(",")
    avg_temp = temperature / count
    print(f"{station_id}\t{month}\t{avg_temp}\t{max_wind}\t{stormy}")


for line in sys.stdin:
    key, value = line.strip().split("\t")
    t, c, w, s = value.split(",")
    t, c, w, s = float(t), int(c), float(w), int(s)

    if key == current_key:
        # same key -> add to the current group
        temperature = temperature + t
        count = count + c
        max_wind = max(max_wind, w)
        stormy = stormy + s
    else:
        # new key -> emit the previous group and start a new one
        if current_key is not None:
            emit(current_key, temperature, count, max_wind, stormy)
        current_key = key
        temperature = t
        count = c
        max_wind = w
        stormy = s

# after the last line: emit the last group (no further key change will trigger it)
if current_key is not None:
    emit(current_key, temperature, count, max_wind, stormy)
