import asyncio
import shlex
import subprocess
import time
from functools import partial
from functools import wraps

import asyncssh
import fabric
from devtools import debug


host = "172.20.0.2"
username = "fractal"
password = "fractal"
CMD = "sleep 3"
BIGDATA = "asd" * 500_000_000

connection_fabric = fabric.Connection(
    host=host,
    user=username,
    connect_kwargs={"password": password},
)
connection_fabric.open()


def async_wrap(func: callable) -> callable:
    """
    Wrap a synchronous callable in an async task.
    """

    @wraps(func)
    async def async_wrapper(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return async_wrapper


async def connect_and_cmd_asyncssh():
    async with asyncssh.connect(
        host,
        username=username,
        password=password,
    ) as connection:
        print("SSH command - START")
        await connection.run(CMD, check=True)
        print("SSH command - END")


async def connect_and_cmd_fabric_vanilla():
    print("SSH command - START")
    connection_fabric.run(CMD)
    print("SSH command - END")


def connect_and_cmd_fabric_vanilla_sync():
    print("SSH command - START")
    connection_fabric.run(CMD)
    print("SSH command - END")


async def connect_and_cmd_fabric_asynchronous():
    print("SSH command - START")
    promise = connection_fabric.run(CMD, asynchronous=True)
    promise.join()
    print("SSH command - END")


async def print_during_sleep():
    for i in range(3):
        print(f"Sleep {i} - START")
        await asyncio.sleep(1)
        print(f"Sleep {i} - END")


async def cpu_intensive():
    print("CPU INTENSIVE - START")
    for i in range(10_000_000):
        a = i
    print("CPU INTENSIVE - END")


async def cpu_intensive_async():
    print("CPU INTENSIVE ASYNC - START")
    for i in range(10_000_000):
        a = i
        await asyncio.sleep(0.0)
    print("CPU INTENSIVE ASYNC - END")


async def subprocess_sleep():
    print("SUBPROCESS - START")
    subprocess.run(shlex.split("sleep 2"), check=True)
    print("SUBPROCESS - END")


async def asyncio_sleep():
    print("ASYNCIO SLEEP - START")
    await asyncio.sleep(3)
    print("ASYNCIO SLEEP - END")


async def time_sleep():
    print("TIME SLEEP - START")
    time.sleep(3)
    print("TIME SLEEP - END")


async def io_write():
    print("IO WRITE - START")
    with open("/tmp/xxx", "w") as f:
        f.write(BIGDATA)
    print("IO WRITE - END")


async def run(function_to_review: callable):
    """
    Run function together with non-blocking function, to review concurrency
    """
    res1 = asyncio.create_task(function_to_review())
    res2 = asyncio.create_task(print_during_sleep())
    await res1
    await res2


# Run the main function in the event loop
if __name__ == "__main__":
    for NAME, FUNCTION in [
        ("CPU INTENSIVE ASYNC", cpu_intensive_async),
        ("IO_WRITE", io_write),
        ("ASYNCIO SLEEP", asyncio_sleep),
        ("TIME SLEEP", time_sleep),
        ("CPU INTENSIVE", cpu_intensive),
        ("SUBPROCESS", subprocess_sleep),
        ("ASYNCSSH", connect_and_cmd_asyncssh),
        ("FABRIC/vanilla", connect_and_cmd_fabric_vanilla),
        ("FABRIC/asynchronous", connect_and_cmd_fabric_asynchronous),
        (
            "FABRIC/asynchronous+wrap",
            async_wrap(connect_and_cmd_fabric_vanilla_sync),
        ),
    ]:
        debug(NAME)
        t0 = time.perf_counter()
        asyncio.run(run(FUNCTION))
        t1 = time.perf_counter()
        debug(NAME, t1 - t0)
        print()
