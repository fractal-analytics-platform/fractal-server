# Fractal Users

Fractal Server's user model and authentication/authorization systems are powered by the [FastAPI Users](https://fastapi-users.github.io/fastapi-users/12.1/) library, and most of the components described below can be identified in the corresponding [overview](https://fastapi-users.github.io/fastapi-users/12.1/configuration/overview).

## User Model
<a name="user-model"></a>

A Fractal user corresponds to an instance of the [`UserOAuth`](http://localhost:8001/reference/fractal_server/app/models/security/#fractal_server.app.models.security.UserOAuth) class, with the following attributes:

| Attribute | Type | Nullable | Default |
| :--- | :---: | :---: | :---: |
| id | integer | | incremental |
| email | email | | - |
| hashed_password | string | | - |
| is_active | bool | | true |
| is_superuser | bool | | false |
| is_verified | bool | | false |
| <span style="background-color:teal;color:white">&nbsp;slurm_user&nbsp;</span> | string | * | null |
| <span style="background-color:teal;color:white">&nbsp;username&nbsp;</span> | string | * | null |
| <span style="background-color:teal;color:white">&nbsp;cache_dir&nbsp;</span> | string | * | null |

The colored attributes are specific for Fractal users, while the other attributes are [provided](https://github.com/fastapi-users/fastapi-users-db-sqlmodel/blob/main/fastapi_users_db_sqlmodel/__init__.py) by FastAPI Users.

In the startup phase, `fractal-server` always creates a default user, who also has the superuser privileges that are necessary for managing other users.
The credentials for this user are defined via the environment variables
[`FRACTAL_ADMIN_DEFAULT_EMAIL`](../../configuration/#fractal_server.config.Settings.FRACTAL_DEFAULT_ADMIN_EMAIL) (default: `admin@fractal.xy`)
and
[`FRACTAL_ADMIN_DEFAULT_PASSWORD`](../../configuration/#fractal_server.config.Settings.FRACTAL_DEFAULT_ADMIN_PASSWORD) (default: `1234`).

> **⚠️ You should always modify the password of the default user after it's created;**
> this can be done with API calls to the `PATCH /auth/users/{id}` endpoint of the [`fractal-server` API](../../openapi), e.g. through the `curl` command or the [Fractal command-line client](https://fractal-analytics-platform.github.io/fractal/reference/fractal/user/#user-edit).
> <mark>When the API instance is exposed to multiple users, skipping the default-user password update leads to a severe vulnerability! </mark>

The most common use cases for `fractal-server` are:

1. The server is used by a single user (e.g. on their own machine, with the [local backend](../runners/local)); in this case you may simply customize and use the default user.
2. The server has multiple users; in this case the admin may use the default user (or another user with superuser privileges) to create additional users (with no superuser privileges). For `fractal-server` to execute jobs on a SLURM cluster (through the corresponding [SLURM backend](../runners/slurm)), each Fractal must be associated to a cluster user via the `slurm_user` attribute (see [here](../runners/slurm/#user-impersonation) for more details about SLURM users).

More details about user management are provided in the [User Management section](#user-management) below.

## Authentication
<a name="authentication"></a>

### Login
<a name="login"></a>

An _authentication backend_ is composed of two parts:

- the <ins>transport</ins>, that manages how the token will be carried over the request,
- the <ins>strategy</ins>, which manages how the token is generated and secured.

Fractal Server provides two authentication backends (Bearer and Cookie), both based the [JWT](https://fastapi-users.github.io/fastapi-users/12.1/configuration/authentication/strategies/jwt/) strategy. Each backend produces both [`/auth/login`](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes/#post-login) and [`/auth/logout`](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes/#post-logout) routes.

> FastAPI Users provides the `logout` endpoint by default, but this is not relevant in `fractal-server` since we do not store tokens in the database.

#### Bearer

The [Bearer](https://fastapi-users.github.io/fastapi-users/12.1/configuration/authentication/transports/bearer/) transport backend provides login at `/auth/token/login`
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

The [Cookie](https://fastapi-users.github.io/fastapi-users/12.1/configuration/authentication/transports/cookie/) transport backend provides login at `/auth/login`

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

Once you have the token, you can use it to identify yourself by sending it along in the header of an API request. Here is an example with an API request to `/auth/whoami`:
```console
$ curl \
    -X GET \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzOTI2MTM4fQ.MqWhW0xRgCV9ZgZr1HcdynrIJ7z46IBzO7pyfTUaTU8" \
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

### OAuth2
<a name="oauth2"></a>

Fractal Server also allows a different authentication procedure, not based on the knowledge of a user's password but on external `OAuth2` authentication clients.

Through the [`httpx-oauth` library](https://frankie567.github.io/httpx-oauth), we currently support `OpenID Connect` (aka `OIDC`), `GitHub` and `Google` (and [many more clients](https://frankie567.github.io/httpx-oauth/oauth2/#provided-clients) can be readily included).

#### Configuration

To use a certain `OAuth2` client, you must first register the `fractal-server` application (see instructions for [GitHub](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/creating-an-oauth-app) and [Google](https://blog.rebex.net/howto-register-gmail-oauth)).
During app registration, you should provide two endpoints:

- the `Homepage URL` (e.g. `http://127.0.0.1:8000/`),
- the `Authorization callback URL` (e.g. `http://127.0.0.1:8000/auth/github/callback`, where `github` could be any client name).

and at the end of this procedure, you will kwnow the _Client ID_ and _Client Secret_ for the app.

> You can have just one `GitHub` client and one `Google` client, but as many `OIDC` client as you want, as long as you call them with different names.

To add an `OAuth2` client, the following environment variables must be added to [`fractal-server` configuration](../../configuration/):

=== "OIDC (single client)"

    ```console
    OAUTH_MYCLIENT_CLIENT_ID=...
    OAUTH_MYCLIENT_CLIENT_SECRET=...
    OAUTH_MYCLIENT_OIDC_CONFIGURATION_ENDPOINT=https://client.com/.well-known/openid-configuration
    OAUTH_MYCLIENT_REDIRECT_URL=...   # e.g. https://fractal-web.example.org/auth/login/oauth2
    ```

=== "OIDC (multiple clients)"

    ```console
    OAUTH_MYCLIENT1_CLIENT_ID=...
    OAUTH_MYCLIENT1_CLIENT_SECRET=...
    OAUTH_MYCLIENT1_OIDC_CONFIGURATION_ENDPOINT=https://client1.com/.well-known/openid-configuration
    OAUTH_MYCLIENT1_REDIRECT_URL=...   # e.g. https://fractal-web.1.example.org/auth/login/oauth2

    OAUTH_MYCLIENT2_CLIENT_ID=...
    OAUTH_MYCLIENT2_CLIENT_SECRET=...
    OAUTH_MYCLIENT2_OIDC_CONFIGURATION_ENDPOINT=https://client2.com/.well-known/openid-configuration
    OAUTH_MYCLIENT2_REDIRECT_URL=...   # e.g. https://fractal-web.2.example.org/auth/login/oauth2
    ```

=== "GitHub"

    ```console
    OAUTH_GITHUB_CLIENT_ID=...
    OAUTH_GITHUB_CLIENT_SECRET=...
    OAUTH_GITHUB_REDIRECT_URL=...   # e.g. https://fractal-web.example.org/auth/login/oauth2
    ```

=== "Google"

    ```console
    OAUTH_GOOGLE_CLIENT_ID=...
    OAUTH_GOOGLE_CLIENT_SECRET=...
    OAUTH_GOOGLE_REDIRECT_URL=...   # e.g. https://fractal-web.example.org/auth/login/oauth2
    ```

When `fractal-server` starts, two new routes will be generated for each client:

- `/auth/client-name/authorize` ,
- `/auth/client-name/callback` (the `Authorization callback URL` of the client).

> For `GitHub` and `Google` clients the `client-name` is `github` or `google`, while for `OIDC` clients it comes from the environment variables (e.g. for `OAUTH_MYCLIENT_CLIENT_ID` the `client-name` is `MYCLIENT`).

> Note that the `OAUTH_*_REDIRECT_URL` environment variable is optional. It is
> not relevant for the examples described in this page, since they are all in
> the command-line interface. However, it is required when OAuth authentication
> is performed starting from a browser (e.g. through the [`fractal-web`
> client](https://github.com/fractal-analytics-platform/fractal-web)), since
> the callback URL should be opened in the browser itself.


#### Authorization Code Flow

Authentication via OAuth2 client is based on the [Authorizion Code Flow](https://auth0.com/docs/get-started/authentication-and-authorization-flow/authorization-code-flow), as described in this diagram
<figure markdown>
  ![Authorization Code Flow](../assets/auth.png)
  <figcaption markdown>(adapted from https://auth0.com/docs/get-started/authentication-and-authorization-flow/authorization-code-flow, © 2023 Okta, Inc.)
  </figcaption>
</figure>

We can now review how `fractal-server` handles these steps:

- **Steps 1 &#8594; 4**<br>
    * The starting point is [`/auth/client-name/authorize`](https://github.com/fastapi-users/fastapi-users/blob/ff9fae631cdae00ebc15f051e54728b3c8d11420/fastapi_users/router/oauth.py#L59);
    * Here an `authorization_url` is generated and provided to the user;
    * This URL will redirect the user to the Authorization Server (which is e.g. GitHub or Google, and not related to `fractal-server`), together with a `state` code for increased security;
    * The user must authenticate and grant `fractal-server` the permissions it requires.

- **Steps 5 &#8594; 8**<br>
    * The flow comes back to `fractal-server` at [`/auth/client-name/callback`](https://github.com/fastapi-users/fastapi-users/blob/ff9fae631cdae00ebc15f051e54728b3c8d11420/fastapi_users/router/oauth.py#L101), together with the Authorization Code.
    * A FastAPI dependency of the callback endpoint, [`oauth2_authorize_callback`](https://github.com/frankie567/httpx-oauth/blob/2e82654559b1687a6b25c86e31dc9290ae06cdba/httpx_oauth/integrations/fastapi.py#L10), takes care of exchanging this code for the Access Token.

- **Steps 9 &#8594; 10**<br>
    * The callback endpoint uses the Access Token to obtain the user's email address and an account identifier from the Resource Server (which, depending on the client, may coincide with the Authorization Server).

After that, the callback endpoint performs some extra operations, which are not stricly part of the `OAuth2` protocol:

- It checks that `state` is still valid;
- If a user with the given email address doesn't already exist, it creates one with a random password;
- If the user has never authenticated with this `OAuth2` client before, it adds in the database a new entry to the `oauthaccount` table, properly linked to the `user_oauth` table`; at subsequent logins that entry will just be updated;
- It prepares a JWT token for the user and serves it in the Response Cookie.

#### Full example

A given `fractal-server` instance is registered as a GitHub App, and `fractal-server` is configured accordingly. A new user comes in, who wants to sign up using her GitHub account (associated to `person@university.edu`).

First, she makes a call to `/auth/github/authorize`:
```
$ curl \
    -X GET \
    http://127.0.0.1:8000/auth/github/authorize

{
    "authorization_url":"https://github.com/login/oauth/authorize?
        response_type=code&
        client_id=...&
        redirect_uri=...&
        state=...&
        scope=user+user%3Aemail"
}
```

Now the `authorization_url` must be visited using a browser.
After logging in to GitHub, she is asked to grant the app the permissions it requires.

After that, she is redirected back to `fractal-server` at `/auth/github/callback`, together with two query parameters:
```
http://127.0.0.1:8000/auth/github/callback?
    code=...&
    state=...
```

The callback function does not return anything, but the response cookie contains a JWT token
```
"fastapiusersauth": {
	"httpOnly": true,
	"path": "/",
	"samesite": "None",
	"secure": true,
	"value": "ey..."     <----- This is the JWT token
}
```

The user can now make [authenticated calls](#authenticated-calls) using this token, as in
```
curl \
    -X GET \
    -H "Authorization: Bearer ey..." \
    http://127.0.0.1:8000/auth/whoami

{
    "id":3,
    "email":"person@university.edu",
    "is_active":true,
    "is_superuser":false,
    "is_verified":false,
    "slurm_user":null,
    "cache_dir":null,
    "username":null
}
```

## Authorization

On top of being authenticated, a user must be _authorized_ in order to perform specific actions in `fratal-server`:

1. Some endpoints require the user to have a specific attribute (e.g. being `active` or being `superuser`);
2. Access control is in-place for some database resources, and encode via database relationships with the User table (e.g. for `Project``);
3. Additional business logic to regulate access may be defined within specific endpoints (e.g. for patching or removing a Task).

The three cases are described more in detail below.

### User attributes

Some endpoints require the user to have a specific attribute.
This is implemented through a FastAPI dependencies, e.g. using [fastapi_users.current_user](https://fastapi-users.github.io/fastapi-users/12.1/usage/current-user/#current_user):
```python
current_active_user = fastapi_users.current_user(active=True)

# fake endpoint
@router.get("/am/i/active/")
async def am_i_active(
    user: UserOAuth = Depends(current_active_user)
):
    return {f"User {user.id}":  "you are active"}
```

Being an _active user_ (i.e. `user.is_active==True`) is required by

- all `/api/v1/...` endpoints
- all `/auth/users/...`,
- POST `/auth/register`,
- GET `/auth/userlist`,
- GET `/auth/whoami`.

Being a _superuser_ (i.e. `user.is_superuser==True`) is required by

- all `/auth/users/...`,
- POST `/auth/register`,
- GET `/auth/userlist`.

and it also gives full access (without further checks) to

- PATCH `/api/v1/task/{task_id}`
- DELETE `/api/v1/task/{task_id}`

No endpoint currently requires the user to be _verified_ (i.e. having `user.is_verified==True`).

### Database relationships

The following resources in the `fractal-server` database are always related to a single `Project` (via their foreign key `project_id`):

- `Dataset`,
- `Workflow`,
- `WorkflowTask` (through `Workflow`).
- `ApplyWorkflow` (i.e. a workflow-execution job),

Each endpoint that operates on one of these resources (or directly on a `Project`) requires the user to be in the `Project.user_list`.

> The `fractal-server` database structure is general, and the user/project relationships is a many-to-many one. However the API does not currently expose a feature to easily associate multiple users to the same project.

### Endpoint logic

The [User Model](#user-model) includes additional attributes `username` and `slurm_user`, which are optional and default to `None`. Apart from `slurm_user` being needed for [User Impersonation in SLURM](../runners/slurm/#user-impersonation), these two attributes are also used for additional access control to `Task` resources.

> ⚠️ This is an experimental feature, which will likely evolve in the future (possibly towards the implementation of user groups/roles).

When a `Task` is [created](https://fractal-analytics-platform.github.io/fractal-server/reference/fractal_server/app/api/v1/task/#fractal_server.app.routes.api.v1.task.create_task), the attribute `Task.owner` is set equal to `username` or, if not present, to `slurm_user` (there must be at least one to create a Task). With a similar logic, we consider a user to be the _owner_ of a Task if `username==Task.owner` or, if `username` is `None`, we check that `slurm_user==Task.owner`.
The following endpoints require a non-superuser user to be the owner of the Task:

- PATCH `/api/v1/task/{task_id}`,
- DELETE `/api/v1/task/{task_id}`.


## User Management
<a name="user-management"></a>

The endpoints to manage users can be found under the route `/auth/`. On top of the `login/logout` ones ([described above](#login)), several other endpoints are available, including all the ones exposed by FastAPI Users (see [here](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes)). Here are more details for the most relevant endpoints.

### POST `/auth/register`

🔐 *Restricted to superusers*.

New users can be registred by a superuser at [`/auth/register`](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes/#register-router):

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

Here we provided `email` and `password`, which are the only required fields of `UserCreate`; we could also provide the following attributes: `is_active`, `is_superuser`, `is_verified`, `slurm_user`, `cache_dir`, `username`.

### GET `/auth/userlist`

🔐 *Restricted to superusers*.

The route `/auth/userlist` returns the list of all registred users:

```console
$ curl \
    -X GET \
    -H "Authorization: Bearer ey..." \
    http://127.0.0.1:8000/auth/userlist

[
    {
        "id":1,
        "email":"admin@fractal.xy",
        "is_active":true,
        "is_superuser":true,
        "is_verified":false,
        "slurm_user":null,
        "cache_dir":null,
        "username":"admin"
    },
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
]
```

### GET `/auth/whoami`

At `/auth/whoami`, we expose a non-superuser-restricted version of "GET ``/auth/users/me`" (described below):
```
curl \
    -X GET \
    -H "Authorization: Bearer ey..." \
    http://127.0.0.1:8000/auth/whoami

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


### `/users` endpoints

🔐 *Restricted to superusers*.

The additional user-management routes exposed by FastAPI Users in `/users` (see [here](https://fastapi-users.github.io/fastapi-users/12.1/usage/routes#users-router)) are available in `fractal-server` at  `/auth/users/`. For the moment all these routes are all restricted to superusers.

**GET `/auth/users/me`**

Returns the current active superuser:
```
curl \
    -X GET \
    -H "Authorization: Bearer ey..." \
    http://127.0.0.1:8000/auth/users/me

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

**PATCH `/me`**

Update the current active superuser. We must provide a `UserUpdate` instance, which is just like a [`UserCreate`](http://127.0.0.1:8001/internals/auth/#register-new-user) except that all attributes are optional.
```console
$ curl \
    -X PATCH \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzNzMzNTY5fQ.ea8wdZHaGYpCwl60pdDBw6BMumc43xcss1rCtaPP1GM" \
    -d '{"slurm_user": "slurm1"}' \
    http://127.0.0.1:8000/auth/users/me

{
    "id":1,
    "email":"admin@fractal.xy",
    "is_active":true,
    "is_superuser":true,
    "is_verified":false,
    "slurm_user":"slurm1",
    "cache_dir":null,
    "username":"admin"
}
```

**GET `/{id}`**

Return the user with a given `id`.

**PATCH `/{id}`**

Update the user with a given `id`.

Requires a `UserUpdate`, like in [PATCH `/me`](http://127.0.0.1:8001/internals/auth/#patch-me).

**DELETE `/{id}`**

Delete the user with the given `id`.
