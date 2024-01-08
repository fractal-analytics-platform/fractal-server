## Requirements

- It requires a .fractal_server.env file with configuration variables
- A database with at least 1 row for each object

## Notes

It is possible to run benchmarks on gunicorn (use `serve.sh` script) deployment.

Executing `bench_api_time.sh` will be execute the `api_bench.py` file with locust cli.
It does some actions:

1. Retrieve all the endpoints from the swagger
2. Filter them based on their action (we keep just the GET endpoints)
3. Subtitute "1" where an url is in the format like `.../{id}/`

Then a `test.html` file will be generated with a summary about response time and number of failures.
