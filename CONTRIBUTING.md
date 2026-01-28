The development of `fractal-server` takes place on the [`fractal-server` Github repository](https://github.com/fractal-analytics-platform/fractal-server). To ask questions or to inform us of a bug or unexpected behavior, please feel free to [open an issue](https://github.com/fractal-analytics-platform/fractal-server/issues/new).

This document describes how to contribute code to the repository.
Other relevant links are:

* [Main webpage of the Fractal project](https://fractal-analytics-platform.github.io)
* [Documentation for `fractal-server`](https://fractal-analytics-platform.github.io/fractal-server)
* [`fractal-server` project on PyPI](https://pypi.org/project/fractal-server)


## Initial setup

### Repository

You can clone the `fractal-server` repository via
```bash
git clone https://github.com/fractal-analytics-platform/fractal-server.git
```
or (see https://docs.github.com/en/get-started/git-basics/about-remote-repositories):
```bash
git clone git@github.com:fractal-analytics-platform/fractal-server.git
```

### `uv`

We use [uv](https://docs.astral.sh/uv/) to manage the development environment and the dependencies - see https://docs.astral.sh/uv/getting-started/installation/ for methods to install it. From the `fractal-server` root folder, you can get started through
```bash
# Create a new virtual environment in `.venv`
uv venv

# Install both the required dependencies and the optional dev/docs dependencies
uv sync --frozen --group dev --group docs

# Run a command from within this environment without updating the `uv.lock` file
uv run --frozen fractalctl --help
```

### `pre-commit`

We use [pre-commit](https://pre-commit.com) to run [several checks](https://github.com/fractal-analytics-platform/fractal-server/blob/main/.pre-commit-config.yaml) on files that are being committed. To set it up locally, you should run
```bash
# Install pre-commit globally
pipx install pre-commit

# Add the pre-commit hook to your local repository
pre-commit install
```

## Development

### `git`

#### Branches and tags

* The default branch for this repository is `main`. This branch is meant to be ready to be released at any time (that is, it should not include code that is knowingly broken), but it is not meant to be stable (that is, it can and it will include breaking changes with respect to previous versions).
* Releases are made by pushing a tag with some specific label (e.g. `1.2.3`), and they are meant to be immutable (that is, we are not planning to ever edit on of these references).
    * The tag creation on GitHub also triggers the creation of a [PyPI release](https://pypi.org/project/fractal-server/#history) - see [the release section below](#release).


#### Pull requests

* Unless the contributed change has an extremely narrow scope or it was already discussed with the Fractal team, please open an issue before working on a new feature.
* Typical code contributions should take the form of Pull Requests (PR) towards the `main` branch.
* When working towards a new complex feature, we sometimes introduce a specific "base" branch (e.g. `dev-new-feature-X`) and we open smaller-scoped PRs towards that base branch. When the core of the new feature is ready, we open a new PR from `dev-new-feature-X` to `main`.
* Opening a PR also creates a corresponding [checklist](https://github.com/fractal-analytics-platform/fractal-server/blob/main/.github/pull_request_template.md).
* Opening a PR towards `main` also triggers several [automated tests](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/).

### Tests

Unit and integration testing of Fractal Server uses the [pytest](https://docs.pytest.org) testing framework.
Typical examples of how to run tests locally are
```bash
# Run all tests
uv run --frozen pytest
```
or
```bash
# Run all tests (more verbose)
uv run --frozen pytest -v -s --log-cli-level info --full-trace
```

[Pytest markers](https://docs.pytest.org/en/stable/example/markers.html) are used to include or exclude some specific tests, notably the ones that require access to some external resources (Docker). An example:
```bash
# Run tests the do not require any Docker container
uv run --frozen pytest -m "not container and not oauth"
```

#### SLURM

To test the SLURM backend, we use a custom version of a Docker local SLURM cluster (defined in [https://github.com/fractal-analytics-platform/fractal-containers](https://github.com/fractal-analytics-platform/fractal-containers)). The pytest plugin [pytest-docker](https://github.com/avast/pytest-docker) is then used to spin up the Docker containers for the duration of the tests.

**Important**: this requires docker being installed on the development system, and the current user being in the `docker` group. A simple check for this requirement is to run a command like `docker ps`, and verify that it does not raise any permission-related error. Note that also `docker compose` must be available.

The specific tests that require a SLURM cluster through `pytest-docker` can be run via
```bash
uv run --frozen pytest -m container
```

#### OAuth and email

For testing OAuth integration and email-sending features, we rely on containerized [Dex](https://dexidp.io) and [mailpit](https://mailpit.axllent.org) services. [A dedicated script](https://github.com/fractal-analytics-platform/fractal-server/blob/main/tools/run_oauth_tests.sh) shows how to run these specific tests (with the appropriate containers and with all relevant configuration variables):
```bash
./tools/run_oauth_tests.sh
```

### Update database schema

Whenever the database schemas in [`app/models`](./reference/app/models/index.md) are modified, you should create a migration script. To check whether this is needed, run
```
uv run --frozen alembic check
```

If needed, the simplest procedure is to use `alembic --autogenerate` to create
an incremental migration script, as in
```
export POSTGRES_DB="autogenerate-fractal-revision"
dropdb --if-exist "$POSTGRES_DB"
createdb "$POSTGRES_DB"
uv run --frozen fractalctl set-db --skip-init-data
uv run --frozen alembic revision --autogenerate -m "Description of the current migration"
```
If successful, this procedure creates a new file in `fractal_server/migrations/versions/`.

### Release

1. Checkout to branch `main` (this is not strictly needed, and a common use case is to make pre-releases from a feature branch).
2. Update the `CHANGELOG.md` file (e.g. remove `(unreleased)` from the upcoming version).
3. If you have modified the models, then you must also [create](#update-database-schema) a new migration script (note: in principle the CI will fail if you forget this step).
4. Use one of the following
```

# Bump version e.g. from 1.2.3a6 to 1.2.3a7
uv run --frozen bumpver update --tag-num --dry

# Bump version e.g. from 1.2.3 to 1.2.4
uv run --frozen bumpver update --patch --dry

# Bump version e.g. from 1.2.3 to 1.3.0
uv run --frozen bumpver update --minor --dry

# Bump version to a specific target X.Y.Z
uv run --frozen bumpver update --set-version X.Y.Z --dry
```
to test updating the version bump.
5. If the previous step looks OK, remove `--dry` and re-run to actually bump the version, commit and push the changes.
6. Approve (or have approved) the new version at [Publish package to PyPI](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/publish_pypi.yml).
7. **After the release**: If the release was a stable one (e.g. `X.Y.Z`, not `X.Y.Za1` or `X.Y.Zrc2`), move `fractal_server/data_migrations/X_Y_Z.py` to `fractal_server/data_migrations/old`.


### Documentation

Documentation for `fractal-server` is hosted at https://fractal-analytics-platform.github.io/fractal-server and built as part of a [dedicated GitHub action](https://github.com/fractal-analytics-platform/fractal-server/blob/main/.github/workflows/documentation.yaml). The code reference is build automatically, based on docstrings and type hints in the Python code.

Docstrings should be formatted as in the [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings).

For building the documentation we use [mkdocs](https://www.mkdocs.org) and the [Material theme](https://squidfunk.github.io/mkdocs-material).

To build the documentation and serve it at http://127.0.0.1:8000:
```bash
# Export some required environment variables
export POSTGRES_DB=mock_fractal
export JWT_SECRET_KEY=mock_fractal

# Build and serve the documentation
uv run --frozen mkdocs serve --config-file mkdocs.yml
```
