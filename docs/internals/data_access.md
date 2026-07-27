# Fractal-data integration

The Fractal backend (`fractal-server`) does not serve scientific data through its API,
but it exposes relevant authentication/authorization details which can be consumed by the
[`fractal-data` data-streaming service](https://github.com/fractal-analytics-platform/fractal-data).

This page describes the `fractal-server` logic, with some additional details on how `fractal-data` consumes its output.

> _NOTE_: In the examples below, ports 8000 and 3000 correspond to `fractal-server` and `fractal-data`, respectively.
> The most relevant backend endpoints are `/auth/current-user/` and `/auth/current-user/allowed-viewer-paths/`, while
> the most relevant data-service endpoint is `/data/files/some/path` (which tries to access the `/some/path` file from
> disk).

## Authentication

As for authentication, `fractal-data` fully delegates it to `fractal-server`.
`fractal-data` parses the HTTP request it receives, looking for the expected Fractal-athenticatio token.
Once the token is found, `fractal-data` makes a request to the `/auth/current-user/` endpoint of `fractal-server`, which behaves as in these examples
```console
# Anonymous user
$ curl http://localhost:8000/auth/current-user/
{"detail":"Unauthorized"}

# Authenticated user
$ curl -s http://localhost:8000/auth/current-user/ -H "Authorization: Bearer ey..." | jq '.'
{
  "id": 1,
  "email": "admin@example.org",
  "is_active": true,
  "is_superuser": true,
  "is_verified": true,
  "is_guest": false,
  "group_ids_names": null,
  "oauth_accounts": [],
  "profile_id": 1,
  "project_dirs": [
    "/tmp/user-project-dir-1",
    "/tmp/user-project-dir-2"
  ],
  "slurm_accounts": []
}
```

If the server does not respond with a valid user, then `fractal-data` also responds with a `401 Unauthorized` response.
```console
# Anonymous user -> 401 response
$ curl -s -i  http://localhost:3000/data/files/some/path/ | head -n1
HTTP/1.1 401 Unauthorized

# Authenticated user -> 403 response (see authorization section below)
$ curl -s -i  http://localhost:3000/data/files/some/path/ -H "Authorization: Bearer ey..." | head -n1
HTTP/1.1 403 Forbidden
```


### Browser usage and zarr viewers

The flow described above is smoother when it takes place within the browser.
The user is typically already interacting with the `fractal-web` web client (e.g. at https://fractal.example.org), and `fractal-data`

Note that this flow is somewhat simplified when it takes place within the browser,
since `fractal-server` and `fractal-data` are typically served on the same domain as the Fractal web client `fractal-web`
(e.g. as , https://fractal.example.org/backend and https://fractal.example.org/data).
In this case, if the user is already interacting with the web-client it means they already have a valid cookie stored in the browser,
which is used for to each one of these services. This


## Authorization


http://localhost:8000:/auth/current-user/

The authentication is fully delegated to `fractal-server`: the data-streaming service forwards HTTP headers/cookies




## Database-stored details


## AP



Database structure

Owned resources

Each user has one or many project directories (database column `UserOAuth.project_dirs`). These are the folders where the user can create new datasets (for zarr files).
Each user can own several datasets, and each dataset has a top-level `zarr_dir` folder. The dataset `zarr_dir` must be a subfolder of one of the user's project directories.

Shared resources

Precise implementation flow is as follows:
```console
# Case 1: Anonymous user cannot access the endpoint

$ curl -i http://localhost:8000/auth/current-user/allowed-viewer-paths/
HTTP/1.1 401 Unauthorized
date: Mon, 27 Jul 2026 06:50:18 GMT
server: uvicorn
content-length: 25
content-type: application/json

{"detail":"Unauthorized"}

# Case 2: A Fractal user (identified by a JWT token) can acecss the endpoint, and get back the list of their project directories
$ curl -i http://localhost:8000/auth/current-user/allowed-viewer-paths/ -H "Authorization: Bearer REDACTED"
HTTP/1.1 200 OK
date: Mon, 27 Jul 2026 06:51:59 GMT
server: uvicorn
content-length: 132
content-type: application/json

["/tmp/user-project-dir-1","/tmp/user-project-dir-2"]

```


(here)[../code_reference/app/routes/auth/viewer_paths/?h=get_current_user_allowed_viewer_paths#fractal_server.app.routes.auth.viewer_paths.get_current_user_allowed_viewer_paths]

This is consumed by
https://github.com/fractal-analytics-platform/fractal-data/blob/main/src/authorizer.ts#L96C1-L96C54
