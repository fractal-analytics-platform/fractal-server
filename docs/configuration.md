To configure the Fractal Server one must define some configuration variables.

Some of them are required, and the server will not start unless they are set.
Some are optional and sensible defaults are provided.

## How to set configuration

There are two possibilities for setting the configuration variables:

- defining them as *environment variables*, in the same environment as Fractal Server:
    ```sh
    export VAR1=value1
    export VAR2=value2
    ```
- write them inside a file called `.fractal_server.env`, located in your current working directory:
    ```
    VAR1=value1
    VAR2=value2
    ```

If the same variable is defined both in the environment and inside the env file, the value defined in the environment takes priority.

Once the variables have been defined in one of these ways, they will be read automatically by the Fractal Server during the start-up phase.

## Minimal working example

This is a minimal working example of a `.fractal_server.env`, with all the required configuration variables:
```txt
POSTGRES_DB=fractal_test
JWT_SECRET_KEY=jwt_secret_key
```
All the other configurations will take the default value.

---

::: fractal_server.config._main
::: fractal_server.config._database
::: fractal_server.config._data
::: fractal_server.config._email
::: fractal_server.config._oauth
