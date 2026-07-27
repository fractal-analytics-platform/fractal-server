# Integration with fractal-data and viewers

The Fractal backend (`fractal-server`) does not serve scientific data through its API,
but it exposes relevant authentication/authorization details which can be consumed by the
[`fractal-data` data-streaming service](https://github.com/fractal-analytics-platform/fractal-data).
The data-streaming service itself is then typically used by viewers (e.g. a hosted
[vizarr](https://github.com/BioNGFF/vizarr) viewer or a local `napari` viewer with the
[`napari-ome-zarr-navigator` plugin](https://github.com/fractal-napari-plugins-collection/napari-ome-zarr-navigator)),
which must make authenticated HTTP requests to `fractal-data` for the files that should be displayed.

This page describes the `fractal-server` logic, with some additional details on how `fractal-data` consumes its output.

> **NOTE 1**: In the examples below, ports 8000 and 3000 correspond to `fractal-server` and `fractal-data`, respectively.
> The most relevant backend endpoints are `http://localhost:8000/auth/current-user/` and `http://localhost:8000/auth/current-user/allowed-viewer-paths/`, while
> the most relevant data-service endpoint is `/data/files/some/path` (which tries to access the `/some/path` file from
> disk).

> **NOTE 2**: The "allowed-viewer-paths" wording is a legacy one, related to the fact that data-streaming
> service is then used to serve data to a viewer. More precise names would be "allowed-paths" or
> "allowed-streaming-paths".


## Authentication

As for authentication, `fractal-data` fully relies on `fractal-server`. As a first step, `fractal-data` parses the
HTTP request it receives, looking for the expected Fractal-athentication cookie or token.
If the token is found, `fractal-data` makes a request to the `/auth/current-user/` endpoint of `fractal-server`,
which behaves as in these examples
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

If the token was not found, or if the server does not respond with a valid user, then `fractal-data` also responds with a `401 Unauthorized` response.
```console
# Anonymous user -> 401 Unauthorized
$ curl -s -i  http://localhost:3000/data/files/some/path/ | head -n1
HTTP/1.1 401 Unauthorized

# Invalid token -> 401 Unauthorized
$ curl -s -i  http://localhost:3000/data/files/some/path/ -H "Authorization: Bearer invalid-token" | head -n1
HTTP/1.1 401 Unauthorized

# Authenticated user requesting an invalid path -> 403 response (more details in the "Authorization" section of this page)
$ curl -s -i  http://localhost:3000/data/files/some/path/ -H "Authorization: Bearer ey..." | head -n1
HTTP/1.1 403 Forbidden

# Authenticated user requesting a valid but missing file -> 404 response
$ curl -s -i http://localhost:3000/data/files/tmp/user-project-dir-1/missing-file.txt -H "Authorization: Bearer ey..." | head -n1
HTTP/1.1 404 Not Found

# Authenticated user requesting a valid existing path -> 200 response (more details in the "Authorization" section of this page)
$ curl -s  http://localhost:3000/data/files/tmp/user-project-dir-1/example.txt -H "Authorization: Bearer ey..."
Some contents
```


### Usage for zarr viewers

The flow described above is smoother (from the user perspective) when it takes place in the browser, and if all Fractal services share
a common domain. When a user is already using the `fractal-web` web client (e.g. at https://fractal.example.org), the browser
storage has a cookie with the Fractal authentication token. This cookie is automatically attached to HTTP requests to `fractal-data`,
if it is hosted e.g. at https://fractal.example.org/data, meaning that the user doesn't have to copy/paste it.

On the other hand, accessing `fractal-data` from a local viewer (e.g. `napari`) requires that the user provides the token themselves.


## Authorization

Here is a recap of the relevant concepts about users and project/dataset ownership, as they are encoded in the Fractal database.

> **Preliminary note**: We use "Fractal user" to describe a user of the Fractal platform
> (whose details are stored in the Fractal database, and whose permissions are defined by Fractal) and "cluster user"
> to described the machine user who exists on the cluster (and whose permissions are defined e.g. at the level of
> filesystem ACLs). As an example, the Fractal user name.surname@example.org may correspond to the `n_surname` cluster
> user.

### Owned vs shared

Each Fractal user is associated to one or several project directories (stored in the database at `UserOAuth.project_dirs` -
which in the examples above takes the value `["/tmp/user-project-dir-1", "/tmp/user-project-dir-2"]`).
These project directories are the only allowed base folders for Fractal-dataset zarr directories, both in terms of metadata (stored in
the database at `Dataset.zarr_dir` - with its value being e.g. `"/tmp/user-project-dir-1/my-dataset`) and in terms of on-disk data (which should
be written within `Dataset.zarr_dir`). For this reason, it makes sense to set the Fractal-user project directories so that the corresponding
cluster user has read-write access to them on-disk.
Note that a dataset with a `Dataset.zarr_dir` which is not a subfolder of any of the user `project_dirs` cannot be created, in Fractal.

Shared resources


[here](../../code_reference/app/routes/auth/viewer_paths/?h=get_current_user_allowed_viewer_paths#fractal_server.app.routes.auth.viewer_paths.get_current_user_allowed_viewer_paths)

This is consumed by
https://github.com/fractal-analytics-platform/fractal-data/blob/main/src/authorizer.ts#L96C1-L96C54
