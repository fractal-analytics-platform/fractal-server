from concurrent.futures import Executor
from typing import Callable

from devtools import debug

from fractal_server.app.runner._slurm.executor import FractalSlurmExecutor


def submit(executor: Executor, fun: Callable, *args, **kwargs):
    try:
        return executor.submit(fun, *args, **kwargs)
    except Exception as e:
        debug(e)
        pass


def test_submit_pre_command(fake_process):
    fake_process.register(["sbatch", fake_process.any()])
    fake_process.register(["sudo", fake_process.any()])

    def foo(a, b):
        return a + b

    with FractalSlurmExecutor() as executor:
        submit(executor, foo, 1, 2)

    call = fake_process.calls.pop()
    assert "sudo" not in call
    assert "sbatch" in call

    USERNAME = "my_user"
    with FractalSlurmExecutor(username=USERNAME) as executor:
        submit(executor, foo, 1, 3)

    call = fake_process.calls.pop()
    assert "sudo" in call
    assert USERNAME in call
    assert "sbatch" in call
