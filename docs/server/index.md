# Fractal Server

Fractal Server is the server side compoenent of Fractal Analytics Platform. It
takes care of

* user management
* project management
* workflows
* running workflows on projects
* expose web API to clients

## Installation

Please refer to

* [Installation](./installation.md)
* [Configuration](./configuration.md)

## Fractal Server internals

The server has a modular structure with several subsystem

* task collection
* [runner backends](runners/index.md)
* [API Auth](./auth.md)
* database interface
