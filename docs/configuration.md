## Set configuration variables

There are two possibilities for setting the configuration variables that determine the settings for `fractal-server`:

1. Define them as *environment variables*, in the same environment where the `fractal-server` process will runs:
    ```sh
    export VARIABLE1=value1
    export VARIABLE2=value2
    ```
- Write them inside a file named `.fractal_server.env`, in your current working directory, with contents like
    ```
    VARIABLE1=value1
    VARIABLE2=value2
    ```

Once the variables have been defined in one of these ways, they will be read automatically by the Fractal Server during the start-up phase.

If the same variable is defined twice, both as an environment variable and inside the `.fractal_server.env` file, the value defined in the environment takes priority.


## Get current configuration

Admins can retrieve the current settings through the appropriate endpoints [`GET /api/settings/`](openapi.md#operations-default-view_settings_api_settings_app__get).


## Configuration variables

Here are all the configuration variables with their description (note: expand the "Source code" blocks to see the default values).

### ::: fractal_server.config._main.Settings
    options:
      show_root_heading: true

### ::: fractal_server.config._database.DatabaseSettings
    options:
      show_root_heading: true

### ::: fractal_server.config._data.DataSettings
    options:
      show_root_heading: true
      filters: ["!check"]

### ::: fractal_server.config._email.EmailSettings
    options:
      show_root_heading: true
      filters: ["!public", "!validate_email_settings"]

### ::: fractal_server.config._oauth.OAuthSettings
    options:
      show_root_heading: true


## Minimal working example

This is a minimal working example of a `.fractal_server.env`, with all the required configuration variables set:
```txt
JWT_SECRET_KEY=secret-key-for-jwt-tokens
POSTGRES_DB=fractal-database-name
```
These are the only required variables. All others, if not specified, will assume their default value.
