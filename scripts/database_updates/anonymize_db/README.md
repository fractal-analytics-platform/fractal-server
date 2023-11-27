1. Start with an active postgresql database, created with fractal-server 1.3.13.
2. Run `python anonymize_db.py`, in an environment where `fractal-server` 1.3.13 is installed.
3. Export the result with
```bash
pg_dump $DB_NAME --format=plain --file=$OUTPUT_FILE
```

What this script does is mainly:
* Remove all users apart from those with ID 1 and 6;
* Rename users 1 and 6 into `admin@example.org` and `user@example.org` (both with password `1234`);
* Redact possibly sensitive information from columns of several tables.
