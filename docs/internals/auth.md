Fractal Server's _user_ model and _authentication/authorization_ system are powered by [FastAPI Users](https://fastapi-users.github.io/).

A user is an instance of the [`UserOAuth`](http://localhost:8001/reference/fractal_server/app/models/security/#fractal_server.app.models.security.UserOAuth) class:

| Attribute | Type | Nullable | Default |
| --- | --- | --- | --- |
| id | integer | | incremental |
| email | email | | |
| hashed_password | string | | |
| is_active | bool | | true |
| is_superuser | bool | | false |
| is_verified | bool | | false |
| slurm_user | string | * | null |
| username | string | * | null |
| cache_dir | string | * | null |

By "default" we mean the values that the attributes assume on initialization if not explicitly set otherwise.

The last three attributes (`slurm_user`, `username` and `cache_dir`) are Fractal specific, all the others are [provided](https://github.com/fastapi-users/fastapi-users-db-sqlmodel/blob/main/fastapi_users_db_sqlmodel/__init__.py) by FastAPI Users.

## Authentication

### Backends

> An _authentication backend_ is composed of two parts:
>
> - the <ins>transport</ins>, that manages how the token will be carried over the request,
> - the <ins>strategy</ins>, which manages how the token is generated and secured.

Fractal Server provides two authentication backends, both using the [JWT](https://fastapi-users.github.io/fastapi-users/10.1/configuration/authentication/strategies/jwt/) strategy.<br>
Each backend produces both [`/login`](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes/#post-login) and [`/logout`](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes/#post-logout) routes.

#### Bearer

The [Bearer](https://fastapi-users.github.io/fastapi-users/10.1/configuration/authentication/transports/bearer/) transport backend provides login at `/auth/token/login`
```console
$ curl \
    -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=admin@fractal.xy&password=1234" \
    http://127.0.0.1:8000/auth/token/login

{
    "access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzNTc1MzM1fQ.UmkhBKxgBM2mxXlrTlt5HXqtDDOe_mMYiMkKUS5jbXU",
    "token_type":"bearer"
}
```

#### Cookie

The [Cookie](https://fastapi-users.github.io/fastapi-users/10.1/configuration/authentication/transports/cookie/) transport backend provides login at `/auth/login`

```console
$ curl \
    -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=admin@fractal.xy&password=1234" \
    --cookie-jar - \
    http://127.0.0.1:8000/auth/login


#HttpOnly_127.0.0.1	FALSE	/	TRUE	0	fastapiusersauth	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzNjQ4MDI5fQ.UKRdbVjwys4grQrhpGyxcxcVbNSNJ29RQiFubpGYYUk
```


### Authenticated calls

Once you have the token, you can use it to identify yourself by sending it along in the header of the request:

```console
$ curl \
    -X GET \
    -H "Authorization: Bearer ey..." \
    http://127.0.0.1:8000/auth/whoami

{
    "id":1,
    "email":"admin@fractal.xy",
    "is_active":true,
    "is_superuser":true,
    "is_verified":false,
    "slurm_user":null,
    "cache_dir":null,
    "username":"admin"
}
```

## Authorization

An authenticated user must be _authorized_ to access specific endpoints.

The user attributes relevant for authorization are:

- `is_active`,
- `is_superuser`,
- `is_verified`,
- `username` / `slurm_user`,
- `id`.

### `is_active`

All `/api/v1/` endpoints first check that the user (regular or superuser) is active, i.e. `is_active==True`.<br>
This is implemented as a FastAPI dependency, using [fastapi_users.current_user](https://fastapi-users.github.io/fastapi-users/10.0/usage/current-user/#current_user):
```python
current_active_user = fastapi_users.current_user(active=True)

# fake endpoint
@router.get("/am/i/active/")
async def am_i_active(
    user: UserOAuth = Depends(current_active_user)
):
    return {f"User {user.id}":  "you are active"}
```
A `401 Unauthorized` will be thrown if the authenticated user is not active.


### `is_superuser`

Being a superuser (i.e. `is_superuser==True`) allows access to

- PATCH `/api/v1/task/{task_id}`
- DELETE `/api/v1/task/{task_id}`

without further checks.


### `is_verified`

No endpoint currently requires `is_verified==True`.


### `username` / `slurm_user`

These are optional attributes, which means they can also be `None`.

When a `Task` is [created](https://fractal-analytics-platform.github.io/fractal-server/reference/fractal_server/app/api/v1/task/#fractal_server.app.api.v1.task.create_task), the attribute `Task.owner` is set equal to `username` or, if not present, to `slurm_user` (there must be at least one to create a ask).<br>
With a similar logic, we consider a user to be the _owner_ of a Task if `username==Task.owner` or, if `username` is `None`, we check that `slurm_user==Task.owner`.

The following endpoints require the user to be a superuser or the owner of the Task:

- PATCH `/api/v1/task/{task_id}`,
- DELETE `/api/v1/task/{task_id}`.

### `id`

Each of these resources in Fractal Server is related to a single `Project`:

- `ApplyWorkflow` (aka Job),
- `Dataset`,
- `Workflow`,
- `WorkflowTask` (actually, related to a single `Workflow`).

As a general rule, each endpoint that operates on one of these resources (or directly on the `Project`) requires the user to be in `Project.user_list`.

## User management

To create a new user the request body must be a `UserCreate`, whereas to modify an existing user we need a `UserUpdate`:

| Attribute | Type | required in: | `UserCreate`  | `UserUpdate` |
| --- | --- | --- | --- | --- |
| email | email | | * | |
| password | string | | * | |
| is_active | bool | | | |
| is_superuser | bool | | | |
| is_verified | bool | | | |
| slurm_user | string | | | |
| username | string | | | |
| cache_dir | string | | | |


### Register new user

The route [`/auth/register`](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes/#register-router) is restricted to superuser and it's used to register a new user:

```console
$ curl \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer ey..." \
    -d '{"email": "user@example.com", "password": "password"}' \
    http://127.0.0.1:8000/auth/register

{
    "id":2,
    "email":"user@example.com",
    "is_active":true,
    "is_superuser":false,
    "is_verified":false,
    "slurm_user":null,
    "cache_dir":null,
    "username":null
}
```

### Reset password

🚧 🏗️

https://fastapi-users.github.io/fastapi-users/12.1/configuration/routers/reset/

### Verify email

🚧 🏗️

https://fastapi-users.github.io/fastapi-users/12.1/configuration/routers/verify/


### Manage users

Users management is under the route `/auth/user/` and it's restricted to superusers.


---


The [API](https://fractal-analytics-platform.github.io/fractal-server/openapi/) exposes two main routes: `/api/` and `/auth/`.


Some "minor" `/auth/` endpoints do not require authentication.<br>
Among the `/api/` endpoints, the only one that does not require authentication is `/api/alive/`.
```console
$ curl http://127.0.0.1:8000/api/alive/

{
    "alive":true,
    "deployment_type":"testing",
    "version":"1.3.5a1"
}
```

Every other endpoint, instead, requires the _authenticated_ user to be _authorized_ to that specific endpoint.





ACTIVE

This dependency is also required by:

- all `/auth/users/...`,
- POST `/auth/register`,
- GET `/auth/whoami`,
- GET `/auth/userlist`.

SUPERUSE

These `/auth/` endpoints use the [`current_active_superuser`](https://github.com/fractal-analytics-platform/fractal-server/blob/main/fractal_server/app/security/__init__.py#L232C11-L232C35) dependency, and therefore require the (active) user to be a superuser:

- all `/auth/users/...`,
- POST `/auth/register`,
- GET `/auth/userlist`.
