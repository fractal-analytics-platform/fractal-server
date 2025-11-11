# Users

Fractal Server's user model and authentication/authorization systems are powered by the [FastAPI Users](https://fastapi-users.github.io/fastapi-users) library, and most of the components described below can be identified in the corresponding [overview](https://fastapi-users.github.io/fastapi-users/latest/configuration/overview).


## ::: fractal_server.app.models.security.UserOAuth
    options:
      show_root_heading: true
      show_root_toc_entry: false
<a name="user-model"></a>

## First user

To manage `fractal-server` you need to create a first user with superuser privileges.
This is done by means of the [`init-db-data`](../cli_reference.md#fractalctl-init-db-data) command together with the`--admin-email` and `--admin-pwd` flags, either during the [startup phase](../../install_and_deploy/#2-initialize-the-database-data) or at a later stage.

The most common use cases for `fractal-server` are:

1. The server is used by a single user (e.g. on their own machine, with the [local backend](runners/local.md)).
    In this case you may simply use the first (and only) user.

2. The server has multiple users, and it is connected to one or more SLURM clusters.
    To execute jobs on a SLURM cluster, a user must be associated to that cluster and to a valid cluster-user via its [`Profile`](../computational/app/models/user_settings.md).
    See [here](runners/slurm.md/#user-impersonation) for more details about SLURM users.


## Authentication
<a name="authentication"></a>

### Login
<a name="login"></a>

An _authentication backend_ is composed of two parts:

- the <ins>transport</ins>, that manages how the token will be carried over the request,
- the <ins>strategy</ins>, which manages how the token is generated and secured.

Fractal Server provides two authentication backends (Bearer and Cookie), both based the [JWT](https://fastapi-users.github.io/fastapi-users/latest/configuration/authentication/strategies/jwt/) strategy. Each backend produces both [`/auth/login`](https://fastapi-users.github.io/fastapi-users/latest/usage/routes/#post-login) and [`/auth/logout`](https://fastapi-users.github.io/fastapi-users/latest/usage/routes/#post-logout) routes.

> FastAPI Users provides the `logout` endpoint by default, but this is not relevant in `fractal-server` since we do not store tokens in the database.

#### Bearer

The [Bearer](https://fastapi-users.github.io/fastapi-users/latest/configuration/authentication/transports/bearer/) transport backend provides login at `/auth/token/login`
```console
$ curl \
    -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=admin@example.org&password=1234" \
    http://127.0.0.1:8000/auth/token/login/

{
    "access_token":"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzNTc1MzM1fQ.UmkhBKxgBM2mxXlrTlt5HXqtDDOe_mMYiMkKUS5jbXU",
    "token_type":"bearer"
}
```

#### Cookie

The [Cookie](https://fastapi-users.github.io/fastapi-users/latest/configuration/authentication/transports/cookie/) transport backend provides login at `/auth/login`

```console
$ curl \
    -X POST \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "username=admin@example.org&password=1234" \
    --cookie-jar - \
    http://127.0.0.1:8000/auth/login/


#HttpOnly_127.0.0.1	FALSE	/	TRUE	0	fastapiusersauth	eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzNjQ4MDI5fQ.UKRdbVjwys4grQrhpGyxcxcVbNSNJ29RQiFubpGYYUk
```

### Authenticated calls

Once you have the token, you can use it to identify yourself by sending it along in the header of an API request. Here is an example with an API request to `/auth/current-user/`:
```console
$ curl \
    -X GET \
    -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIyIiwiYXVkIjpbImZyYWN0YWwiXSwiZXhwIjoxNjkzOTI2MTM4fQ.MqWhW0xRgCV9ZgZr1HcdynrIJ7z46IBzO7pyfTUaTU8" \
    http://127.0.0.1:8000/auth/current-user/

{
    "id":1,
    "email":"admin@example.org",
    "is_active":true,
    "is_superuser":true,
    "is_verified":false,
    "slurm_user":null,
    "cache_dir":null
}
```

### OAuth2
<a name="oauth2"></a>

Fractal Server also allows a different authentication procedure, not based on the knowledge of a user's password but on external `OAuth2` authentication clients.

Through the [`httpx-oauth` library](https://frankie567.github.io/httpx-oauth), we currently support `OpenID Connect` (aka `OIDC`), `GitHub` and `Google` (and [more clients](https://frankie567.github.io/httpx-oauth/reference/httpx_oauth.clients/) can be readily included).

#### Configuration

To use a certain `OAuth2` client, you must first register the `fractal-server` application (see instructions for [GitHub](https://docs.github.com/en/apps/oauth-apps/building-oauth-apps/creating-an-oauth-app) and [Google](https://blog.rebex.net/howto-register-gmail-oauth)).



During app registration, you should provide two endpoints:

- the `Homepage URL` (e.g. `http://127.0.0.1:8000/`),
- the `Authorization callback URL` (e.g. `http://127.0.0.1:8000/auth/github/callback/`, where `github` could be any client name).

and at the end of this procedure, you will kwnow the _Client ID_ and _Client Secret_ for the app.

> Note: You have to enable the "Email addresses" permission for your GitHub registered app, at https://github.com/settings/apps/{registered-app}/permissions. A similar setting may be required for Google.


To add an `OAuth2` client, you must provide valid a [`OAuthSettings`](../configuration.md#fractal_server.config._oauth.OAuthSettings):

=== "OIDC"

    ```console
    OAUTH_CLIENT_NAME=any-name-except-github-or-google
    OAUTH_CLIENT_ID=...
    OAUTH_CLIENT_SECRET=...
    OAUTH_OIDC_CONFIG_ENDPOINT=...  # e.g. https://example.org/.well-known/openid-configuration
    OAUTH_REDIRECT_URL=...          # e.g. https://fractal-web.example.org/auth/login/oauth2
    ```

=== "GitHub"

    ```console
    OAUTH_CLIENT_NAME=github
    OAUTH_CLIENT_ID=...
    OAUTH_CLIENT_SECRET=...
    OAUTH_REDIRECT_URL=...  # e.g. https://fractal-web.example.org/auth/login/oauth2
    ```

=== "Google"

    ```console
    OAUTH_CLIENT_NAME=google
    OAUTH_CLIENT_ID=...
    OAUTH_CLIENT_SECRET=...
    OAUTH_REDIRECT_URL=...  # e.g. https://fractal-web.example.org/auth/login/oauth2
    ```

When `fractal-server` starts with proper [`OAuthSettings`](../configuration.md#fractal_server.config._oauth.OAuthSettings), two new routes will be generated:

- `/auth/{OAUTH_CLIENT_NAME}/authorize` ,
- `/auth/{OAUTH_CLIENT_NAME}/callback` (the `Authorization callback URL` of the client).

> Note that the `OAUTH_REDIRECT_URL` environment variable is optional. It is
> not relevant for the examples described in this page, since they are all in
> the command-line interface. However, it is required when OAuth authentication
> is performed starting from a browser (e.g. through the [`fractal-web`
> client](https://fractal-analytics-platform.github.io/fractal-web/oauth2/)), since
> the callback URL should be opened in the browser itself.


#### Authorization Code Flow

Authentication via OAuth2 client is based on the [Authorization Code Flow](https://auth0.com/docs/get-started/authentication-and-authorization-flow/authorization-code-flow), as described in this diagram
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

After that, the callback endpoint performs some extra operations, which are not strictly part of the `OAuth2` protocol:

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
    http://127.0.0.1:8000/auth/github/authorize/

{
    "authorization_url":"https://github.com/login/oauth/authorize/?
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
http://127.0.0.1:8000/auth/github/callback/?
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
    http://127.0.0.1:8000/auth/current-user/

{
    "id":3,
    "email":"person@university.edu",
    "is_active":true,
    "is_superuser":false,
    "is_verified":false,
    "slurm_user":null,
    "cache_dir":null
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
This is implemented through a FastAPI dependencies, e.g. using [fastapi_users.current_user](https://fastapi-users.github.io/fastapi-users/latest/usage/current-user/#current_user):
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

- all `/api/v2/...` endpoints
- all `/auth/users/...`,
- POST `/auth/register/`,
- GET `/auth/userlist/`,
- GET `/auth/current-user/`.

Being a _superuser_ (i.e. `user.is_superuser==True`) is required by

- all `/auth/users/...`,
- POST `/auth/register/`,
- GET `/auth/userlist/`.

and it also gives full access (without further checks) to

- PATCH `/api/v2/task/{task_id}/`
- DELETE `/api/v2/task/{task_id}/`

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

The [User Model](#user-model) includes additional attributes `username` and `slurm_user`, which are optional and default to `None`. Apart from `slurm_user` being needed for [User Impersonation in SLURM](runners/slurm.md#user-impersonation), these two attributes are also used for additional access control to `Task` resources.

> ⚠️ This is an experimental feature, which will likely evolve in the future (possibly towards the implementation of user groups/roles).

When a `Task` is created, the attribute `Task.owner` is set equal to `username` or, if not present, to `slurm_user` (there must be at least one to create a Task). With a similar logic, we consider a user to be the _owner_ of a Task if `username==Task.owner` or, if `username` is `None`, we check that `slurm_user==Task.owner`.
The following endpoints require a non-superuser user to be the owner of the Task:

- PATCH `/api/v2/task/{task_id}/`,
- DELETE `/api/v2/task/{task_id}/`.
