The development of Fractal Server takes place
[on Github](https://github.com/fractal-analytics-platform/fractal-server).

To contribute code, please fork the repository and submit a pull request.

## Setting up the development environment

Fractal uses [poetry](https://python-poetry.org/docs/) to manage the
development environment and the dependencies. Running

```
poetry install --with dev [-E <extras>]
```

will initialise a Python virtual environment and install Fractal Server and
all its dependencies, including development dependencies.

It may sometimes be useful to use a different Python interpreter from the one
installed in your system. To this end we suggest using
[pyenv](https://github.com/pyenv/pyenv). In the project folder, invoking

```
pyenv local <version>
poetry env use <version>
```

will install Fractal in a development environment using an interpreter pinned
at the version provided instead of the system interpreter.

## Testing

Unit and integration testing of Fractal Server uses the [pytest](https://docs.pytest.org/en/7.1.x/) testing framework.

If you installed the development dependencies, you may run
the test suite by invoking

```
poetry run pytest
```

Some runner backends and database engines may benefit from dummy services
(such as a database or a virtual SLURM cluster). These are automatically set
up using `pytest-docker`, which relies on `docker` and `docker-compose` being
installed on the development system.

## Building documentation

1. Setup a python environment and install the requirements from [`docs/doc-requirements.txt`](https://github.com/fractal-analytics-platform/fractal-server/blob/main/docs/doc-requirements.txt).
2. Run `mkdocs serve --config-file mkdocs.yml` and browse documentation at `http://127.0.0.1:8000`.
