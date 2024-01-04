#!/bin/bash

locust -f api_bench.py \
--host http://127.0.0.1:8000 \
--users 1 \
--spawn-rate 10 \
--run-time 10s \
--headless \
--html test.html
