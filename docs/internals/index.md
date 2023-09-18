# Fractal Server internal components

On top of exposing a web API to clients, `fractal-server` includes several internal subsystems:

* [Database interface](database_interface.md), supporting SQLite and PostgreSQL;
* [Basic logging](logs.md);
* [Automated task collection](task_collection.md) for Fractal-compatible task packages;
* [User management, authentication and authorization](users.md);
* [Computational backends](runners/index.md) to execute workflows;
