import json
import sys
import time
from datetime import datetime

component = sys.argv[1]  # This what we want to parallelize over
sleep_time = float(sys.argv[2])  # How long to sleep
outdir = sys.argv[3]  # Where to store results

ref_time = datetime(2023, 3, 1, 1, 1, 1)
start_time = datetime.now()
print(f"{start_time}, START for {component=} and {sleep_time=}")
time.sleep(sleep_time)
end_time = datetime.now()
print(f"{end_time}, END   for {component=} and {sleep_time=}")

start_time_sec = (start_time - ref_time).total_seconds()
end_time_sec = (end_time - ref_time).total_seconds()

fmt = "%Y-%m-%d %H:%M:%S,%f"
res = dict(
    component=component,
    sleep_time=sleep_time,
    start_time=start_time.strftime(fmt),
    start_time_sec=start_time_sec,
    end_time_sec=end_time_sec,
    end_time=end_time.strftime(fmt),
)
with open(f"{outdir}/results_{component}.json", "w") as f:
    json.dump(res, f, indent=2, sort_keys=True)
