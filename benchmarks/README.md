## Requirements

- It requires a .fractal_server.env file with configuration variables
- A database with at least 1 row for Project, Dataset and Resources

## Notes

It is possible to run benchmarks on gunicorn (use `serve.sh` script) deployment.
It also populates the database with some mock data to perform further benchmarks.

Executing:

```bash
poetry run locust -f api_bench.py \
        --host http://0.0.0.0:8000 \
        --users 1 \
        --spawn-rate 10 \
        --run-time 10s \
        --headless \
        --html bench.html
```

It does some actions:

1. Retrieve all the endpoints from the swagger
2. Filter the endpoints, based on their action (we keep just the GET endpoints)
3. Subtitute the right number where an url is in the format like `.../{id}/`

Then a `bench.html` file will be generated with a summary about response time and number of failures.
