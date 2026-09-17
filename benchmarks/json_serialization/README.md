Benchmarks in this folder are relevant in the context of https://github.com/fractal-analytics-platform/fractal-server/pull/3418, where we migrate from SQLModel to plain SQLAlchemy.
Als




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

```console
$ pwd
/redacted/fractal-server/benchmarks/json_serialization

$ git show --oneline -s
78abf91120 (HEAD -> 3413-explore-sqlmodel-sqlalchemy-migration) more assertions

$ POSTGRES_DB=123 JWT_SECRET_KEY=123 uv run python bench_orm_object_dump_sqlalchemy.py
[  UserOAuth]: mean=2.39 ns  median=2.22 ns  min=2.07 ns  max=3.89 ns
[TaskGroupV2]: mean=3.44 ns  median=2.99 ns  min=2.83 ns  max=6.97 ns

$ git switch main
Switched to branch 'main'
Your branch is up to date with 'origin/main'.

$ git switch -c tmp-branch
Switched to a new branch 'tmp-branch'

$ git checkout  3413-explore-sqlmodel-sqlalchemy-migration .
Updated 5 paths from 3e9adc91ba

$ git status
On branch tmp-branch
Changes to be committed:
  (use "git restore --staged <file>..." to unstage)
	new file:   README.md
	new file:   bench_json_serializer.py
	new file:   bench_orm_object_dump_sqlalchemy.py
	new file:   bench_orm_object_dump_sqlmodel.py
	new file:   utils_for_orm_dump.py

$ git show --oneline -s
5d4f994946 (HEAD -> tmp-branch, origin/main, origin/HEAD, main, benchmark-sqlmodel-model-dump) Merge pull request #3437 from fractal-analytics-platform/dependabot/uv/version-updates-a88bc2595b

[to be continued]
```


Known differences:
* The old (SQLModel/Pydantic) approach would raise a (relevant) warning.
* We need to work with db-committed ORM objects, or there would be two different ways of setting default values.
