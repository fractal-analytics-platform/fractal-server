import logging
import sys
import time

component = sys.argv[1]  # This what we want to parallelize over
sleep_time = float(sys.argv[2])  # How long to sleep

fmt = "%Y-%m-%d %H:%M:%S,%f"
start_time = time.time()
print(start_time)

logging.basicConfig(format="%(asctime)s; %(message)s")
logging.warning(f"{component=}, {sleep_time=}; START")
time.sleep(sleep_time)
logging.warning(f"{component=}, {sleep_time=}; END")
