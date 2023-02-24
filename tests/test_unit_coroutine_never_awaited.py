import asyncio

import pytest


async def test_warning():
    # Raises UserWarning
    raise UserWarning("This will raise no error")


@pytest.mark.xfail()
async def test_fail_explicit_RuntimeWarning():
    raise RuntimeWarning("asd")


@pytest.mark.xfail()
async def test_fail_implicit_RuntimeWarning():
    asyncio.sleep(0.2)
