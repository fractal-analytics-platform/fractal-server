The development of Fractal Server takes place on the [fractal-server Github repository](https://github.com/fractal-analytics-platform/fractal-server).  To ask questions or to inform us of a bug or unexpected behavior, please feel free to [open an issue](https://github.com/fractal-analytics-platform/fractal-server/issues/new).

To contribute code, please fork the repository and submit a pull request.

## Set up the development environment

### Install poetry

Fractal uses [poetry](https://python-poetry.org/docs) to manage the development environment and dependencies, and to streamline the build and release operations; at least version 2.0.0 is recommended.

A simple way to install `poetry` is
```console
pipx install poetry==2.2.1`
```
while other options are described [here](https://python-poetry.org/docs#installing-with-the-official-installer).


### Clone repository

You can clone the `fractal-server` repository via
```
git clone https://github.com/fractal-analytics-platform/fractal-server.git
```

### Install package

Running
```
poetry install --with dev --with docs
```
will initialise a Python virtual environment and install Fractal Server and all its dependencies, including optional dependencies. Note that to run commands from within this environment you should prepend them with `poetry run` (as in `poetry run fractalctl set-db`).

## Update database schema

Whenever the models in [`app/models`](./reference/app/models/index.md) are modified, you should update them via a migration. To check whether this is needed, run
```
poetry run alembic check
```

If needed, the simplest procedure is to use `alembic --autogenerate` to create
an incremental migration script, as in
```
$ export POSTGRES_DB="autogenerate-fractal-revision"
$ dropdb --if-exist "$POSTGRES_DB"
$ createdb "$POSTGRES_DB"
$ poetry run fractalctl set-db --skip-init-data
$ poetry run alembic revision --autogenerate -m "Some migration message"
```

## Release

1. Checkout to branch `main`.
2. Check that the current HEAD of the `main` branch passes all the tests (note: make sure that you are using the poetry-installed local package).
3. Update the `CHANGELOG.md` file (e.g. remove `(unreleased)` from the upcoming version).
4. If you have modified the models, then you must also [create](#update-database-schema) a new migration script (note: in principle the CI will fail if you forget this step).
5. Use one of the following
```
poetry run bumpver update --tag-num --tag-commit --commit --dry
poetry run bumpver update --patch --tag-commit --commit --dry
poetry run bumpver update --minor --tag-commit --commit --dry
poetry run bumpver update --set-version X.Y.Z --tag-commit --commit --dry
```
to test updating the version bump.
6. If the previous step looks OK, remove `--dry` and re-run to actually bump the version, commit and push the changes.
7. Approve (or have approved) the new version at [Publish package to PyPI](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/publish_pypi.yml).
8. **After the release**: If the release was a stable one (e.g. `X.Y.Z`, not `X.Y.Za1` or `X.Y.Zrc2`), move `fractal_server/data_migrations/X_Y_Z.py` to `fractal_server/data_migrations/old`.


## Run tests

Unit and integration testing of Fractal Server uses the [pytest](https://docs.pytest.org/en/7.1.x/) testing framework.

To test the SLURM backend, we use a custom version of a  Docker local SLURM cluster. The pytest plugin [pytest-docker](https://github.com/avast/pytest-docker) is then used to spin up the Docker containers for the duration of the tests.

**Important**: this requires docker being installed on the development system, and the current user being in the `docker` group. A simple check for this requirement is to run a command like `docker ps`, and verify that it does not raise any permission-related error. Note that also `docker compose` must be available..

If you installed the development dependencies, you may run the test suite by invoking
```
poetry run pytest
```
from the main directory of the `fractal-server` repository. It is sometimes useful to specify additional arguments, e.g.
```
poetry run pytest -s -v --log-cli-level info --full-trace
```

Tests are also run as part of [GitHub Actions Continuous Integration](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/ci.yml) for the `fractal-server` repository.


## Documentation

The documentations is built with [mkdocs](https://www.mkdocs.org) and the [Material theme](https://squidfunk.github.io/mkdocs-material). Docstrings should be formatted as in the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).

To build the documentation

1. Setup a python environment and install the requirements from [`docs/doc-requirements.txt`](https://github.com/fractal-analytics-platform/fractal-server/blob/main/docs/doc-requirements.txt).
2. Run
```
poetry run mkdocs serve --config-file mkdocs.yml
```
and browse the documentation at `http://127.0.0.1:8000`.
