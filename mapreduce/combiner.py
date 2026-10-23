#!/usr/bin/env python3
# Combiner: partial aggregation on the worker, before the shuffle.
# Input and output use the mapper format:  station_id,month \t temp_sum,count,max_wind,stormy
# It never divides (an average of averages is wrong) - it passes sums and counts on to the reducer.
import sys

current_key = None
temperature = 0.0
count = 0
max_wind = 0.0
stormy = 0


def emit(key, temperature, count, max_wind, stormy):
    print(f"{key}\t{temperature},{count},{max_wind},{stormy}")


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

# after the last line: emit the last group
if current_key is not None:
    emit(current_key, temperature, count, max_wind, stormy)
