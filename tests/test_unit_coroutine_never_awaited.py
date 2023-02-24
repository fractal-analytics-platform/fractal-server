import asyncio

import pytest


@pytest.mark.skip
async def test_warning():
    """
    This raises a UserWarning, which is not included in filterwarnings and
    therefore not transformed into an error.
    """
    raise UserWarning("This is a dummy warning, to be ignored")


@pytest.mark.xfail()
async def test_fail_explicit_RuntimeWarning():
    """
    This raises a "genunine" RuntimeWarning, which is included in
    filterwarnings and transformed into an error.
    """
    raise RuntimeWarning("asd")


@pytest.mark.xfail()
async def test_fail_implicit_RuntimeWarning():
    """
    This (indirectly) raises a RuntimeWarning because the coroutine 'sleep' was
    never awaited. Note that this is likely an unhandled warning exception, see
    multiple references mentioned in
    https://github.com/fractal-analytics-platform/fractal-server/issues/287.
    The presence of both RuntimeWarning and
    pytest.PytestUnraisableExceptionWarning in filterwarnings transforms it
    into an error.
    """
    asyncio.sleep(0.2)
