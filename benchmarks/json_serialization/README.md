Benchmarks in this folder are relevant in the context of https://github.com/fractal-analytics-platform/fractal-server/pull/3418, where we migrate from SQLModel to plain SQLAlchemy.

## (1) Object serialization to JSON

A customized `json.dumps` function (which handles some special binary types) is used twice in the PR #318:
* As the `json_serializer` argument for SQLAlchemy-engine creation, so that JSON/JSONB colums can be created in the ORM by providing Python objects (including with attributes of type `datetime.datetime`, `uuid.UUID` or `pathlib.Path`).
* As part of the function that replaces `SQLModel.model_dump` (see section 2 below)

The test can be run as in
```bash
uv run bench_json_serializer.py
```

An example of output (as of cef0a63af4) looks like
```
Generated nested structure with 100 nodes.
[ json.dumps]: mean=0.07 ms  median=0.07 ms  min=0.07 ms  max=0.10 ms
[TypeAdapter]: mean=0.03 ms  median=0.03 ms  min=0.03 ms  max=0.04 ms
TypeAdapter speed-up ratio: 2.65x

Generated nested structure with 1000 nodes.
[ json.dumps]: mean=0.49 ms  median=0.56 ms  min=0.29 ms  max=0.60 ms
[TypeAdapter]: mean=0.18 ms  median=0.15 ms  min=0.13 ms  max=0.27 ms
TypeAdapter speed-up ratio: 2.67x

Generated nested structure with 10102 nodes.
[ json.dumps]: mean=2.90 ms  median=2.86 ms  min=2.84 ms  max=3.18 ms
[TypeAdapter]: mean=1.41 ms  median=1.40 ms  min=1.39 ms  max=1.46 ms
TypeAdapter speed-up ratio: 2.07x

Generated nested structure with 100000 nodes.
[ json.dumps]: mean=28.97 ms  median=28.89 ms  min=28.60 ms  max=30.44 ms
[TypeAdapter]: mean=13.48 ms  median=13.18 ms  min=13.07 ms  max=21.23 ms
TypeAdapter speed-up ratio: 2.15x
```


## (2) ORM-object serialization

ORM objects of the `sqlmodel.SQLModel` type have a `model_dump` method based on `pydantic`, which is sometimes used in `fractal-server` to produce a Python-dictionary version of the ORM object.
As part of https://github.com/fractal-analytics-platform/fractal-server/pull/3418, we implemented an equivalent function for `sqlalchemy.DeclarativeBase` objects.

> **Note**: In both cases, the resulting dictionary may not be JSON-serializable by a vanilla `json.dumps`, e.g. because it contains some `pathlib.Path` or `datetime.datetime` objects.

Here we compare the former method (by working on the current `main` branch) and the new one (by working in the current branch as part of https://github.com/fractal-analytics-platform/fractal-server/pull/3418).

Note that we use the database-committed objects. This is to avoid a discrepancy about when defaults are populated (which for SQLModel takes place uponc object construction, while for SQLAlchemy takes place in the databse).

```
$ pwd
/redacted/fractal-server/benchmarks/json_serialization

$ git show --oneline -s
5a5c1f3418 (HEAD -> 3413-explore-sqlmodel-sqlalchemy-migration) Use proper SQLModel base

$ dropdb --if-exists json-benchmarks

$ createdb json-benchmarks

$ POSTGRES_HOST=/var/run/postgresql POSTGRES_DB=json-benchmarks JWT_SECRET_KEY=123 uv run python bench_orm_object_dump_sqlalchemy.py
      Built fractal-server @ file:///home/tommaso/Fractal/fractal-server                                                                                                                                                         Uninstalled 1 package in 0.35ms
Installed 1 package in 1ms
[   Resource]: mean=2.38 ns  median=2.33 ns  min=2.24 ns  max=3.01 ns
[    Profile]: mean=2.15 ns  median=1.96 ns  min=1.90 ns  max=5.03 ns
[  UserOAuth]: mean=2.32 ns  median=2.18 ns  min=1.94 ns  max=4.45 ns
[TaskGroupV2]: mean=3.67 ns  median=3.24 ns  min=3.02 ns  max=7.45 ns

$ git checkout main ../../fractal_server/
Updated 85 paths from 80623de687

$ POSTGRES_HOST=/var/run/postgresql POSTGRES_DB=json-benchmarks JWT_SECRET_KEY=123 uv run python bench_orm_object_dump_sqlmodel.py
[   Resource]: mean=2.80 ns  median=2.70 ns  min=2.64 ns  max=3.67 ns
[    Profile]: mean=2.28 ns  median=2.11 ns  min=2.03 ns  max=5.20 ns
[  UserOAuth]: mean=2.53 ns  median=2.38 ns  min=2.26 ns  max=5.09 ns
[TaskGroupV2]: mean=6.67 ns  median=6.43 ns  min=6.30 ns  max=10.56 ns


$ git restore --staged ../../fractal_server/

$ git restore  ../../fractal_server/

$ git status
On branch 3413-explore-sqlmodel-sqlalchemy-migration
Your branch is ahead of 'origin/3413-explore-sqlmodel-sqlalchemy-migration' by 17 commits.
  (use "git push" to publish your local commits)

nothing to commit, working tree clean
```
