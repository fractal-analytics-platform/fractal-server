import pytest
from devtools import debug
from pydantic.error_wrappers import ValidationError

from fractal_server.common.schemas import ProjectCreate


def test_project_create():
    # Successful creation
    p = ProjectCreate(name="my project")
    debug(p)
    # Check that whitespaces are stripped from beginning/end of string
    NAME = "some project name"
    p = ProjectCreate(name=f"  {NAME}  ")
    debug(p)
    assert p.name == NAME
    # Fail due to empty string
    with pytest.raises(ValidationError):
        ProjectCreate(name="  ")
