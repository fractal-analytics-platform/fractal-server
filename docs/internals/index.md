# Fractal Server

Fractal Server is the server-side component of the Fractal Analytics Platform.
It takes care of

* user management
* project management
* workflows
* running workflows on a computational backend
* expose web API to clients

To get started with Fractal Server, refer to

* [Install and deploy](../install_and_deploy.md)
* [Configuration](../configuration.md)

## Fractal Server internals

The server has a modular structure with several subsystem

* [task collection](task_collection.md)
* [computational backends](runners/index.md)
* [API Auth](auth.md)
* [database interface](database_interface.md)
