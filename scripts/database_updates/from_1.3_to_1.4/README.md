Start with a postgresql database created from fractal-server 1.3.13.

Install fractal-server 1.4.0.

Apply migrations via
```bash
fractalctl set-db
```

Backup current DB with
```bash
pg_dump $DB_NAME --format=plain --file=$OUTPUT_FILE
```

Run
```bash
python fix_db.py
```

Install fractal-server 1.4.1

Apply migrations via
```bash
fractalctl set-db
```
