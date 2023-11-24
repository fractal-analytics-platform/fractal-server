Start with a postgresql database created from fractal-server 1.3.13.
Apply `anonymize_db.py`.
Export the result with
```bash
pg_dump $DB_NAME --format=plain --file=$OUTPUT_FILE
```
