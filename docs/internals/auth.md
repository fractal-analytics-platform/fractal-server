## Authorization

The only publically available endpoint of the Fractal Server API is `/api/alive/`:
```console
$ curl http://127.0.0.1:8000/api/alive/

{
    "alive":true,
    "deployment_type":"testing",
    "version":"1.3.5a1"
}
```

Every other `/api/` endpoint (and some `/auth/` endpoints) requires the authenticated user who's making the call to be _authorized_ to that specific endpoint.

A user in Fractal Server is an instance of [UserOAuth](http://localhost:8001/reference/fractal_server/app/models/security/#fractal_server.app.models.security.UserOAuth).<br>
The user attributes relevant for authorization are:

- `is_active`,
- `is_superuser`,
- `is_verified`,
- `username` / `slurm_user`,
- `id`.

#### `is_active`

All `/api/` endpoints first check that the user (regular or superuser) is active, i.e. `is_active==True`.<br>
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

This dependency is also required by:

- all `/auth/users/...`,
- POST `/auth/register`,
- GET `/auth/whoami`,
- GET `/auth/userlist`.

#### `is_superuser`

Among the `/api/` endpoints, `is_superuser==True` allows access to

- PATCH `/api/v1/task/{task_id}`
- DELETE `/api/v1/task/{task_id}`

without further checks.<br>
The check is made inside the auxiliary function [`_get_task_check_owner`](https://fractal-analytics-platform.github.io/fractal-server/reference/fractal_server/app/api/v1/_aux_functions/#fractal_server.app.api.v1._aux_functions._get_task_check_owner).

These `/auth/` endpoints use the [`current_active_superuser`](https://github.com/fractal-analytics-platform/fractal-server/blob/main/fractal_server/app/security/__init__.py#L232C11-L232C35) dependency, and therefore require the (active) user to be a superuser:

- all `/auth/users/...`,
- POST `/auth/register`,
- GET `/auth/userlist`.

#### `is_verified`

No endpoint currently requires `is_verified==True`.
