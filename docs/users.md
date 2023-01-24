The Fractal Server and Client offer a set of features to manage users
(largely based on
[fastapi-users](https://fastapi-users.github.io/fastapi-users)); the typical
use cases are:

1. The server has access to a SLURM cluster, through the
   [SLURM backend](../internals/runners/slurm), and it has multiple users - who
   are also associated to SLURM users.
2. The server is used by a single user on their own machine, with the
   [local backend](../internals/runners/local/).


In all cases, the server startup automatically creates a default user, who also
has the superuser privileges that are necessary for managing other users.
The credentials for this user are:
```
username=admin@fractal.xy
password=1234
```
or any value that is provided via the environment variables
[`FRACTAL_ADMIN_DEFAULT_EMAIL`](../configuration/#fractal_server.config.Settings.FRACTAL_DEFAULT_ADMIN_EMAIL)
and
[`FRACTAL_ADMIN_DEFAULT_PASSWORD`](../configuration/#fractal_server.config.Settings.FRACTAL_DEFAULT_ADMIN_PASSWORD).

Any other user-management action has to take place through the Fractal Client,
and especially via its [`fractal user`
command](https://fractal-analytics-platform.github.io/fractal/cli_files/user.html).

## Single user

If no one else has access to the machine where Fractal Server is running (e.g.
your own machine), you may stick with using the default user.
In this case, you may proceed using the [Fractal
Client](https://fractal-analytics-platform.github.io/fractal) to define and run
workflows, using the standard credentials.

> <mark> **_NOTE:_**  Even on your own machine, it is good practice to always
> modify the credentials of the default user, through the [`fractal user edit`
> command](https://fractal-analytics-platform.github.io/fractal/cli_files/user.html#edit).
> </mark>


## Multiple users

If Fractal Server is deployed on a SLURM cluster, then each SLURM user who
wants to use Fractal needs to have a user registered on the Fractal Server.


1. After server startup, the first necessary step is to modify the credentials of the default user,
through the [`fractal user edit`
command](https://fractal-analytics-platform.github.io/fractal/cli_files/user.html#edit).

    > <mark> **_NOTE:_** Skipping the password-update step leads to a severe
    > vulnerability! </mark>

2. After updating the default user (either `admin@fractal.xy` or the one that
   you chose via environment variables), you can register other Fractal Server
    users via the Fractal Client, through the [`fractal user register`
    command](https://fractal-analytics-platform.github.io/fractal/cli_files/user.html#register).

For the full descriptions of user-management commands see the [`fractal user`
docs](https://fractal-analytics-platform.github.io/fractal/cli_files/user.html).
Note that only a superuser can run these commands (apart from the `fractal
whoami` one, which is open to any registered user).
