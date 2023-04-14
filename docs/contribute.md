The development of Fractal Server takes place on the [fractal-server Github
repository](https://github.com/fractal-analytics-platform/fractal-server).  To
ask questions or to inform us of a bug or unexpected behavior, please feel free
to [open an
issue](https://github.com/fractal-analytics-platform/fractal-server/issues/new).

To contribute code, please fork the repository and submit a pull request.

## Set up the development environment

### Install poetry

Fractal uses [poetry](https://python-poetry.org/docs) to manage the development
environment and dependencies, and to streamline the build and release
operations. Version 1.3 is recommended, although 1.2.2 should also work.

A simple way to install it is the command `pipx install poetry==1.3`; other
options are described
[here](https://python-poetry.org/docs#installing-with-the-official-installer).


### Clone repositories

You can clone the `fractal-server` repository via
```
git clone https://github.com/fractal-analytics-platform/fractal-server.git
```
and then (from the `fractal-server` folder)
```
git submodule update --init
```
The second command is needed, since `fractal-server` includes
[`fractal-common`](https://github.com/fractal-analytics-platform/fractal-common)
as a git submodule.

### Install package

Running
```
poetry install
```
will initialise a Python virtual environment and install Fractal Server and all
its dependencies, including development dependencies.
Note that to run commands from within this environment you should prepend them
with `poetry run`, as in `poetry run fractalctl start`.

To install Fractal Server with some additional extras, use the [`-E`
option](https://python-poetry.org/docs/pyproject/#extras), as in
```
poetry install -E slurm
poetry install -E slurm -E postgres
poetry install --all-extras
```

It may sometimes be useful to use a different Python interpreter from the one
installed in your system. To this end we suggest using
[pyenv](https://github.com/pyenv/pyenv). In the project folder, invoking
```
pyenv local <version>
poetry env use <version>
```
will install Fractal in a development environment using an interpreter pinned
at the version provided instead of the system interpreter.

## Update database schema

Whenever the models are modified (either in
[`app/models`](../reference/fractal_server/app/models/) or in
[`common/schemas`](../reference/fractal_server/common/schemas)), you should
update them via a migration. The simplest procedure is to use `alembic
--autogenerate` to create an incremental migration script, as in the following
```
$ export SQLITE_PATH=some-test.db
$ rm some-test.db
$ poetry run fractalctl set-db
$ poetry run alembic revision --autogenerate -m "Some migration message"

# UserWarning: SQLite is partially supported but discouraged in production environment.SQLite offers partial support for ForeignKey constraints. As such, consistency of the database cannot be guaranteed.
# INFO  [alembic.runtime.migration] Context impl SQLiteImpl.
# INFO  [alembic.runtime.migration] Will assume non-transactional DDL.
# INFO  [alembic.autogenerate.compare] Detected added column 'task.x'
#   Generating /some/path/fractal_server/migrations/versions/155de544c342_.py ...  done
```

## Build and release

Preliminary check-list

* The `main` branch is checked out.
* You reviewed dependencies, and the lock file is up to date with ``pyproject.toml``.
* The current HEAD of the `main` branch passes all the tests (note: make sure
  that you are using the poetry-installed local package).
* You updated the `CHANGELOG.md` file.
* You [updated the schema
  version](./#generate-database-migration-script) (if needed).

Actual **release instructions**:

1. Use:
```
poetry run bumpver update --[tag-num|patch|minor] --tag-commit --commit --dry
```
to test updating the version bump.

2. If the previous step looks good, remove `--dry` and re-run to actually bump the
version and commit the changes locally.

3. Test the build with:
```
poetry build
```
4. If the previous step was successful, push the version bump and tags:
```
git push && git push --tags
```
5. Finally, publish the updated package to PyPI with:
```
poetry publish --dry-run
```
replacing ``--dry-run`` with ``--username YOUR_USERNAME --password
YOUR_PASSWORD`` when you made sure that everything looks good.


## Run tests

Unit and integration testing of Fractal Server uses the
[pytest](https://docs.pytest.org/en/7.1.x/) testing framework.

To test the SLURM backend, we use a custom version of a [Docker local SLURM
cluster](https://github.com/rancavil/slurm-cluster). The pytest plugin
[pytest-docker](https://github.com/avast/pytest-docker) is then used to spin up
the Docker containers for the duration of the tests.

**Important**: this requires docker being installed on the development system,
and the current user being in the `docker` group. A simple check for this
requirement is to run a command like `docker ps`, and verify that it does not
raise any permission-related error. Note that also `docker-compose` must be
available, but this package is installed as a dependency of `pytest-docker`
(when it is installed with the extra `docker-compose-v1`, as in the case of
Fractal Server).


If you installed the development dependencies, you may run
the test suite by invoking
```
poetry run pytest
```
from the main directory of the `fractal-server` repository. It is sometimes
useful to specify additional arguments, e.g.
```
poetry run pytest -s -vvv --log-cli-level info --full-trace
```

Tests are also run as part of [GitHub Actions Continuous
Integration](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/ci.yml)
for the `fractal-server` repository.


## Documentation

The documentations is built with [mkdocs](https://www.mkdocs.org) and the
[Material theme](https://squidfunk.github.io/mkdocs-material).  Whenever
possible, docstrings should be formatted as in the [Google Python Style
Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).


To build the documentation

1. Setup a python environment and install the requirements from
   [`docs/doc-requirements.txt`](https://github.com/fractal-analytics-platform/fractal-server/blob/main/docs/doc-requirements.txt).
2. Run
```
poetry run mkdocs serve --config-file mkdocs.yml
```
and browse the documentation at `http://127.0.0.1:8000`.
