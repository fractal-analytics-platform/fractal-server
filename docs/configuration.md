To configure the Fractal Server one must define some configuration variables.

Some of them are required, and the server will not start unless they are set.
Some are optional and sensible defaults are provided.

## How to set configuration

There are two possibilities for setting the configuration variables:

- defining them as *environment variables* in the same environment as Fractal Server
    ```sh
    export VAR1=value1
    export VAR2=value2
    fractalctl start
    ```
- write them inside a file called `.fractal_server.env`, located in your current working directory
    ```sh
    echo "VAR1=value1
    VAR2=value2" > .fractal_server.env
    fractalctl start
    ```

## Minimal working example

This is a minimal working example of a `.fractal_server.env`, with all the required configuration variables:
```txt
POSTGRES_DB=fractal_test
JWT_SECRET_KEY=jwt_secret_key
```
All the other configurations will assume the defaul value.

---

::: fractal_server.config._main
::: fractal_server.config._database
::: fractal_server.config._data
::: fractal_server.config._email
::: fractal_server.config._oauth
