## Requirements

- It requires a .fractal_server.env file with configuration variables
- A database with at least 1 row for Project, Dataset and Resources

## Notes

It is possible to run benchmarks on gunicorn (use `serve.sh` script) deployment.
It also populates the database with some mock data to perform further benchmarks.

Executing:

Run:

```bash
uv run --frozen  python api_bench.py
```

Then the `bench.html` file will be generated with a summary about response time and number of failures.
