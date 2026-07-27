# Integration with fractal-data and viewers

The Fractal backend (`fractal-server`) does not serve scientific data through its API,
but it exposes relevant authentication/authorization details which can be consumed by the
[`fractal-data` data-streaming service](https://github.com/fractal-analytics-platform/fractal-data).
The data-streaming service itself is then typically called by viewers (e.g. a hosted
[vizarr](https://github.com/BioNGFF/vizarr) viewer or a local `napari` viewer with the
[`napari-ome-zarr-navigator` plugin](https://github.com/fractal-napari-plugins-collection/napari-ome-zarr-navigator)),
which must make authenticated HTTP requests to `fractal-data` for the files that should be displayed.

This page describes the `fractal-server` logic, and the additional details on how `fractal-data` consumes its output.

> **NOTE 1**: In the examples below, ports 8000 and 3000 correspond to `fractal-server` and `fractal-data`, respectively.
> The most relevant backend endpoints are http://localhost:8000/auth/current-user/ and
> http://localhost:8000/auth/current-user/allowed-viewer-paths/, while the most relevant data-service endpoint is
> http://localhost:3000/data/files/some/path (which tries to access the `/some/path` file from disk).

> **NOTE 2**: The "allowed-viewer-paths" wording is a legacy one, related to the fact that data-streaming
> service is then used to serve data to a viewer. More precise names would be "allowed-paths" or
> "allowed-streaming-paths".


## Authentication

As for authentication, `fractal-data` fully relies on `fractal-server`. As a first step, `fractal-data` parses the
HTTP request it receives, looking for the expected Fractal-authentication cookie or token.
If the token is found, `fractal-data` makes a request to the `/auth/current-user/` endpoint of `fractal-server`,
which behaves as in these examples:

* Anonymous user:
```console
$ curl http://localhost:8000/auth/current-user/
{"detail":"Unauthorized"}
```

* Authenticated user:
```console
$ curl -s http://localhost:8000/auth/current-user/ -H "Authorization: Bearer ey..." | jq '.'
{
  "id": 1,
  "email": "name.surname@example.org",
  "is_active": true,
  "is_superuser": false,
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

Here is a list of example behaviors:

* Anonymous user gets a 401-Unauthorized response:
```console
$ curl -s -i  http://localhost:3000/data/files/some/path/ | head -n1
HTTP/1.1 401 Unauthorized
```
* Invalid token leads to a 401-Unauthorized response:
```console
$ curl -s -i  http://localhost:3000/data/files/some/path/ -H "Authorization: Bearer invalid" | head -n1
HTTP/1.1 401 Unauthorized
```
* Authenticated user requesting an invalid path gets a 403-Forbidden response (more details in the [Authorization section](#authorization) below):
```console
$ curl -s -i  http://localhost:3000/data/files/some/path/ -H "Authorization: Bearer ey..." | head -n1
HTTP/1.1 403 Forbidden
```
* Authenticated user requesting a valid but missing file gets a 404-Not-found response (more details in the [Authorization section](#authorization) below):
```console
$ curl -s -i http://localhost:3000/data/files/tmp/user-project-dir-1/missing-file.txt -H "Authorization: Bearer ey..." | head -n1
HTTP/1.1 404 Not Found
```
* Authenticated user requesting a valid existing file gets the file contents (more details in the [Authorization section](#authorization) below):
```console
$ curl -s  http://localhost:3000/data/files/tmp/user-project-dir-1/test.txt -H "Authorization: Bearer ey..."
Some file contents
```


### Usage for OME-Zarr viewers

The flow described above is smoother (from the user perspective) when it takes place in the browser, and if all Fractal services share
a common domain. When a user is already using the `fractal-web` web client (e.g. at https://fractal.example.org), the browser
has a cookie with the Fractal authentication token. This cookie is automatically attached to HTTP requests to `fractal-data`,
if it is hosted e.g. at https://fractal.example.org/data, meaning that the user doesn't have to copy/paste it.

On the other hand, accessing `fractal-data` from a local viewer (e.g. `napari`) requires that the user provides the token themselves -
possibly through an additional plugin like
[`napari-ome-zarr-navigator`](https://github.com/fractal-napari-plugins-collection/napari-ome-zarr-navigator).


## Authorization

Here is a recap of the relevant concepts about users and project/dataset ownership in `fractal-server`, and a
description of how this affects the `fractal-data` authorization layer.

### Context: Actors and permissions

A "Fractal user" is a user who can interact with the Fractal platform;
their details are stored in the Fractal database, and their permissions are defined by Fractal.
Each Fractal user is associated to a "cluster user", that is, is a machine user who exists on the cluster;
their permissions are defined by the cluster admins e.g. through UNIX groups and/or filesystem ACLs.
As an example, the Fractal user name.surname@example.org may correspond to the `n_surname` cluster user.

The Fractal platform runs through a privileged service user, which has some (limited) impersonation rights
(see e.g. [the supported-approaches details](./integrations/#supported-integrations)).
Depending on the deployment details, this service user may also have broad data access.

The `fractal-server` Fractal backend does not include a supported way to expand a Fractal-user's permissions
beyond the ones of their cluster user. As an example, if the `n_surname` cluster user has no read or read-write
access to `/tmp/some-file`, then there is no supported way they can run a job orchestrated by `fractal-server`
which grants them access to that file.

The `fractal-data` data-streaming service, on the other hand, _may_ provide read access to `/tmp/some-data` to the
name.surname@example.org Fractal user, even in a situation where the `n_surname` cluster user does not have on-disk
access to it. The rest of this section describes the context and details of how the Fractal platform (notably
through its `fractal-server` and `fractal-data` components) implements the authorization scheme for this use case.

### Context: Owned vs shared projects

A Fractal user can own a list of projects, and each project includes a series of datasets.

On top of the projects/datasets they own, Fractal users can also interact with projects/datasets where they are _guests_,
meaning that the project owner shared the project with them. A project-sharing invitation has to be explicitly accepted
by the guest, and can be revoked by the project owner at any time.

### Context: Typical on-disk access patterns

Each Fractal user is associated to one or several project directories (stored in the database at `UserOAuth.project_dirs` -
which in the examples above takes the value `["/tmp/user-project-dir-1", "/tmp/user-project-dir-2"]`).

> **Note**: Changing the list of project directories of a Fractal user is an operation which is restricted to Fractal admin users.

These project directories are the only allowed base folders for Fractal-dataset zarr directories, both in terms of Fractal metadata
(stored in the database at `Dataset.zarr_dir` - with a valid example being `"/tmp/user-project-dir-1/my-dataset` and an invalid
example being `/tmp/somewhere-else/my-dataset`) and in terms of on-disk OME-Zarr data (which should be written within `Dataset.zarr_dir`).
For this reason, it makes sense that the project directories of a given Fractal user are read-write-accessible on-disk for the
corresponding cluster user.

> **Note**: Fractal does not let a user create a dataset with a `Dataset.zarr_dir` which is not a subfolder of any of their `project_dirs`.

Regarding shared projects, a meaningful on-disk access pattern depends on the kind of collaboration which is meant to happen:

* If both the project owner and the guest are expected to run jobs that modify data on-disk, then it's meaningful that both their cluster users have
  read/write on-disk access.
* If the guest is only meant to look at data, without running jobs on the cluster, then the corresponding on-disk access is not required.

### `fractal-data` authorization logic

When an authenticated user makes a request to `fractal-data` (e.g. `GET http://localhost:3000/data/files/some/path/`),
the service relies on the [`authorizer` module](https://github.com/fractal-analytics-platform/fractal-data/blob/main/src/authorizer.ts)
to decide whether this specific user should have access to the specific `/some/path` file
(provided that the file exists and is accessible to the `fractal-data` service user).

This decision consists in a request to the `fractal-server` endpoint at `/auth/current-user/allowed-viewer-paths/`.
In its default behavior, this endpoint returns a list made of two kinds of paths: (1) All the project directories of the current user, and (2) the zarr directories of all datasets which are accessible to the current user (either as a project owner or as a project guest).
Here are some examples:

* The current user has two project directories and no datasets:
```console
$ curl -s http://localhost:8000/auth/current-user/allowed-viewer-paths/ -H "Authorization: Bearer ey..." | jq '.'
[
  "/tmp/user-project-dir-1",
  "/tmp/user-project-dir-2"
]
```

* Same as above, but the current user also owns a dataset with `zarr_dir="/tmp/user-project-dir-1/my-dataset-1"`:
```console
$ curl -s http://localhost:8000/auth/current-user/allowed-viewer-paths/ -H "Authorization: Bearer ey..." | jq '.'
[
  "/tmp/user-project-dir-1",
  "/tmp/user-project-dir-2",
  "/tmp/user-project-dir-1/my-dataset-1"
]
```

* Same as above, but a second user created a dataset with `zarr_dir="/tmp/user-project-dir-2/another-dataset"` and shared it with the current user. Note that for this to happen both users should have `/tmp/user-project-dir-2` as one of their admin-provided project directories.
```console
$ curl -s http://localhost:8000/auth/current-user/allowed-viewer-paths/ -H "Authorization: Bearer ey..." | jq '.'
[
  "/tmp/user-project-dir-1",
  "/tmp/user-project-dir-2",
  "/tmp/user-project-dir-1/my-dataset-1",
  "/tmp/user-project-dir-2/another-dataset"
]
```

> **Note**: Here is the [implementation of the `/auth/current-user/allowed-viewer-paths/` endpoint](../code_reference/app/routes/auth/viewer_paths/?h=get_current_user_allowed_viewer_paths#fractal_server.app.routes.auth.viewer_paths.get_current_user_allowed_viewer_paths).

After this request to `fractal-server`, `fractal-data` finds out whether the requested-file path is a subpath of one of the authorized paths
(as provided by `fractal-server`):

* If not, it responds with a 403-Forbidden status.
* If yes, it tries to read and serve the file. If the file exists and can be read, the response contains its contents. Other possible responses are 404-not-found (path was valid, but file does not exist), 400-bad-request (path is a directory), 500-internal-error (file is not readable).

> **Note**: The `fractal-server` response for a given user (identified by their valid Fractal token)
> is kept in cache for `CACHE_EXPIRATION_TIME` seconds (with the default of this `fractal-data`
> configuration variable being 60 seconds). Therefore it may take as long as `CACHE_EXPIRATION_TIME`
> seconds before a backend-side update (e.g. revoking access to a shared project) is reflected into
> `fractal-data` behavior.
