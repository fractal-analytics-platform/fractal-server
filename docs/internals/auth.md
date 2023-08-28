# Auth subsystem

Fractal server uses an OAuth2 compatible subsystem for
authentication/authorisation.

For the implementation, refer to the
[security](../../reference/fractal_server/app/security/) module.

---

_August 2023_

We have implemented, as an example, login via GitHub using OAuth2.

### Step 1: register the app

A new OAuth App must be registered [here](https://github.com/settings/developers), providing the homepage of our FastAPI app (in our example, `localhost:8000`) and the callback endpoint
```
Homepage URL = http://127.0.0.1:8000/
Authorization callback URL = http://127.0.0.1:8000/auth/github/callback
```

After the registration we must have two strings: the `ClientId` and the `ClientSecret`.

> Note that the callback URL has `/github/` in its route. This name is in no way related to the GitHub site, but is a name we chose through the env file, which we will see in the next step.

### Step 2: tell FastAPI about the OAuth client

Before starting the FastAPI app, we must add the following two lines to `.fractal_server.env`
```
OAUTH_GITHUB_CLIENT_ID=
OAUTH_GITHUB_CLIENT_SECRET=
```
using the `ClientId` and the `ClientSecret` from the previous step.

> If we had added
> ```
> OAUTH_XYZ_CLIENT_ID=
> OAUTH_XYZ_CLIENT_SECRET=
> ```
> everything would have worked the same way, with the only difference being that our callback url would have been `http://127.0.0.1:8000/auth/xyz/callback`.

### Step 3: login via GitHub

At this stage, after `fractalctl start`, the only account in the database is the admin account (specified via the env file with `FRACTAL_ADMIN_DEFAULT_EMAIL` and `FRACTAL_ADMIN_DEFAULT_PASSWORD`):

`select * from user_oauth;`

| id |      email  |  hashed_password | is_active | is_superuser | is_verified | slurm_user | cache_dir | username |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | admin@fractal.xy | $2b$12$lDKk89n/FXPe3SH40f6RKeyWKyx/PeCfeF4cGD799dxZTIvFosEkO | t         | t            | f           | |           | admin |

(1 row)

`select * from oauthaccount;`

| id | user_id | oauth_name | access_token | expires_at | refresh_token | account_id | account_email |
| --- | --- | --- | --- | --- | --- | --- | --- |

(0 rows)

We have to distinguish between two cases:
- an already registered user who wants to log in with GitHub;
- a user registering for the first time in fractal using GitHub.

#### Case 1

To see what happens in the first case, we register a user (from Fractal Client) using the same mail as his GitHub account:
```console
$ fractal user register fractal.user@fakemail.com password

{
  "id": 2,
  "email": "fractal.user@fakemail.com",
  "is_active": true,
  "is_superuser": false,
  "is_verified": false,
  "slurm_user": null,
  "cache_dir": null,
  "username": null
}
```
Now

`select * from user_oauth;`

| id |      email  |  hashed_password | is_active | is_superuser | is_verified | slurm_user | cache_dir | username |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | admin@fractal.xy | $2b$12$lDKk89n/FXPe3SH40f6RKeyWKyx/PeCfeF4cGD799dxZTIvFosEkO | t         | t            | f           | |           | admin |
| 2 | fractal.user@fakemail.com | $2b$12$J2RZ2tHRHw65NYNGJbXkDeyNhAixpUEer/899hrp9dxXd1.bWPqoy | t         | f            | f |            |           | |

(2 rows)

`select * from oauthaccount;`

| id | user_id | oauth_name | access_token | expires_at | refresh_token | account_id | account_email |
| --- | --- | --- | --- | --- | --- | --- | --- |

(0 rows)

Now let's suppose that the user want's to log in via GitHub.<br>
He visits http://127.0.0.1:8000/auth/github/authorize and recieves an `authorization_url`.<br>
Visiting that url, he must log in using his GitHub account (fractal.user@fakemail.com and his GitHub password) and recieves back a token in the response cookie.

Now we have this situation

`select * from oauthaccount;`

| id | user_id | oauth_name | access_token | expires_at | refresh_token | account_id | account_email |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 2 | github     |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 |            |               | 143396502  | fractal.user@fakemail.com |

(1 row)

`SELECT user_oauth.id, oauthaccount.id, oauthaccount.user_id`<br>
`FROM user_oauth`<br>
`JOIN oauthaccount ON user_oauth.id = oauthaccount.user_id;`


| id | id | user_id |
| --- | --- | --- |
| 2 | 1 | 2 |

(1 row)

> That `access_token` in the db it's the hashed version of the one the user got from the response cookie.

Since we used in the code `associate_by_email=True`, there is no need to add more: the previously registred account is now associated with his OAuth account.


#### Case 2

A completly new user (with a GitHub account, let's say new_user@google.com) visits http://127.0.0.1:8000/auth/github/authorize and recieves his token as before.

The situation of the db now it's the following

`select * from user_oauth;`

| id |      email  |  hashed_password | is_active | is_superuser | is_verified | slurm_user | cache_dir | username |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | admin@fractal.xy | $2b$12$lDKk89n/FXPe3SH40f6RKeyWKyx/PeCfeF4cGD799dxZTIvFosEkO | t         | t            | f           | |           | admin |
| 2 | fractal.user@fakemail.com | $2b$12$J2RZ2tHRHw65NYNGJbXkDeyNhAixpUEer/899hrp9dxXd1.bWPqoy | t         | f            | f |            |           | |
| 3 | new_user@google.com | $2b$12$7SrzxTsSnxWlSPyKCHB3gO6AZbmwyYXczx5UuNDczFCVfTN9JrRKO | t         | f            | f           |            | |

(3 rows)



`select * from oauthaccount;`

| id | user_id | oauth_name | access_token | expires_at | refresh_token | account_id | account_email |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 2 | github     |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 |            |               | 143396502  | fractal.user@fakemail.com |
| 2 |       3 | github     | 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 |            | | 32713010   | new_user@google.com
(2 row)

`SELECT user_oauth.id, oauthaccount.id, oauthaccount.user_id`<br>
`FROM user_oauth`<br>
`JOIN oauthaccount ON user_oauth.id = oauthaccount.user_id;`
| id | id | user_id |
| --- | --- | --- |
| 2 |  1 | 2 |
| 3 |  2 | 3 |
(2 rows)

A new `user_oauth` and a new `oauthaccount` are created in the db, with the right relationship between them.


### Step 4: use the OAuth account

From now on, if the app recieves a call with header
```
{
    'Authorization': 'Bearer ...token..."
}
```
where the token is the one generated from GitHub during the login, the call will be considered authorized.
