import asyncio
from functools import partial
from functools import wraps
from typing import Callable


def async_wrap(func: Callable) -> Callable:
    """
    Wrap a synchronous callable in an async task

    Ref: [issue #140](https://github.com/fractal-analytics-platform/fractal-server/issues/140)
    and [this StackOverflow answer](https://stackoverflow.com/q/43241221/19085332).

    Returns:
        async_wrapper:
            A factory that allows wrapping a blocking callable within a
            coroutine.
    """  # noqa: E501

    @wraps(func)
    async def async_wrapper(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return async_wrapper
