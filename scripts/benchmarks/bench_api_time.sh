locust -f general_bench.py \
--host http://0.0.0.0:8000 \
--users 1 \
--spawn-rate 10 \
--run-time 10s \
--headless \
--html test.html
