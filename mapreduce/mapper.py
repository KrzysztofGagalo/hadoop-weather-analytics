#!/usr/bin/env python3
# Mapper: one weather measurement (CSV line) -> key/value pair
#   key:   station_id,month              (what we group by)
#   value: temperature,1,wind_speed,stormy  (what we aggregate; 1 = measurement counter)
import sys

for line in sys.stdin:
    fields = line.strip().split(",")

    # skip the CSV header and malformed lines
    if fields[0] == "measurement_id" or len(fields) != 8:
        continue

    station_id = fields[1]
    month = fields[2][:7]                       # "2024-05-10T05:00" -> "2024-05"
    temperature = fields[3]
    wind_speed = fields[5]

    stormy = 1 if fields[7] == "Stormy" else 0  # summing these gives the number of storms

    # Hadoop Streaming treats everything before the first tab as the key
    print(f"{station_id},{month}\t{temperature},1,{wind_speed},{stormy}")
