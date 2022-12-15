# Fractal Server

[![PyPI version](https://img.shields.io/pypi/v/fractal-server?color=gree)](https://pypi.org/project/fractal-server/)
[![CI Status](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/ci.yml/badge.svg)](https://github.com/fractal-analytics-platform/fractal-server/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-BSD_3--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

Fractal is a framework to process high content imaging data at scale and prepare it for interactive visualization.

![Fractal_Overview](https://fractal-analytics-platform.github.io/assets/fractal_overview.jpg)

This is the server component of the fractal analytics platform. If you are interested in the client component, please refer to the [main
repository](https://github.com/fractal-analytics-platform/fractal). If you are interested in the fractal tasks, please refer to [the tasks repository](https://github.com/fractal-analytics-platform/fractal-tasks-core).

## Documentation

See https://fractal-analytics-platform.github.io/fractal-server.

## Development

### Setting up environment

We use [poetry](https://python-poetry.org/docs/) (v1.2) to manage the development environment and the dependencies. Running
```
poetry install [--with dev] [-E slurm]
```
will take care of installing all the dependencies in a separate environment,
optionally installing also the development dependencies.

It may be useful to use a different interpreter from the one installed in your
system. To this end we recommend using
[pyenv](https://github.com/pyenv/pyenv). In the project folder, invoking
```
pyenv local 3.8.13
poetry env use 3.8
poetry install
```
will install Fractal in a development environment using `python 3.8.13` instead
of the system-wide interpreter.

### Testing

We use [pytest](https://docs.pytest.org/en/7.1.x/) for unit and integration
testing of Fractal. If you installed the development dependencies, you may run
the test suite by invoking
```
poetry run pytest
```

# Contributors and license

Unless otherwise stated in each individual module, all Fractal components are released according to a BSD 3-Clause License, and Copyright is with Friedrich Miescher Institute for Biomedical Research and University of Zurich.

The SLURM compatibility layer is based on [`clusterfutures`](https://github.com/sampsyo/clusterfutures), by [@sampsyo](https://github.com/sampsyo) and collaborators, and it is released under the terms of the MIT license.

Fractal was conceived in the Liberali Lab at the Friedrich Miescher Institute for Biomedical Research and in the Pelkmans Lab at the University of Zurich (both in Switzerland). The project lead is with [@gusqgm](https://github.com/gusqgm) & [@jluethi](https://github.com/jluethi). The core development is done under contract by [@mfranzon](https://github.com/mfranzon), [@tcompa](https://github.com/tcompa) & [@jacopo-exact](https://github.com/jacopo-exact) from [eXact lab S.r.l.](exact-lab.it).
