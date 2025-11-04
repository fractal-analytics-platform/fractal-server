import tomllib

import pytest
import tomli_w

from fractal_server.tasks.v2.utils_pixi import simplify_pyproject_toml


def test_simplify_pyproject_toml():
    # Invalid TOML
    with pytest.raises(tomllib.TOMLDecodeError):
        simplify_pyproject_toml(
            original_toml_string="invalid contents",
            pixi_environment="x",
            pixi_platform="x",
        )

    # No pixi/tools path
    old = tomli_w.dumps({"some-key": "some-value"})
    new = simplify_pyproject_toml(
        original_toml_string=old,
        pixi_environment="x",
        pixi_platform="x",
    )
    assert old == new

    # Only workspace/platforms update / OLD PIXI SYNTAX
    old = tomli_w.dumps(
        dict(tool=dict(pixi=dict(project=dict(platforms=["a", "b", "c"]))))
    )
    expected_new = tomli_w.dumps(
        dict(tool=dict(pixi=dict(project=dict(platforms=["linux-64"]))))
    )
    new = simplify_pyproject_toml(
        original_toml_string=old,
        pixi_environment="x",
        pixi_platform="linux-64",
    )
    assert new == expected_new

    # Only workspace/platforms update / OLD PIXI SYNTAX
    old = tomli_w.dumps(
        dict(tool=dict(pixi=dict(workspace=dict(platforms=["a", "b", "c"]))))
    )
    expected_new = tomli_w.dumps(
        dict(tool=dict(pixi=dict(workspace=dict(platforms=["linux-64"]))))
    )
    new = simplify_pyproject_toml(
        original_toml_string=old,
        pixi_environment="x",
        pixi_platform="linux-64",
    )
    assert new == expected_new

    # Only project/platforms update
    old = tomli_w.dumps(
        dict(tool=dict(pixi=dict(project=dict(platforms=["a", "b", "c"]))))
    )
    expected_new = tomli_w.dumps(
        dict(tool=dict(pixi=dict(project=dict(platforms=["linux-64"]))))
    )
    new = simplify_pyproject_toml(
        original_toml_string=old,
        pixi_environment="x",
        pixi_platform="linux-64",
    )
    assert new == expected_new

    # Only environments update
    old = tomli_w.dumps(
        dict(tool=dict(pixi=dict(environments=dict(default="fake", y="fake"))))
    )
    expected_new = tomli_w.dumps(
        dict(tool=dict(pixi=dict(environments=dict(default="fake"))))
    )

    new = simplify_pyproject_toml(
        original_toml_string=old,
        pixi_environment="default",
        pixi_platform="x",
    )
    assert new == expected_new

    # No 'default' environment
    old = tomli_w.dumps(
        dict(tool=dict(pixi=dict(environments=dict(x="x", y="y"))))
    )
    with pytest.raises(
        ValueError,
        match="No 'default' pixi environment",
    ):
        simplify_pyproject_toml(
            original_toml_string=old,
            pixi_environment="default",
            pixi_platform="x",
        )

    # Only tasks update
    old = tomli_w.dumps(dict(tool=dict(pixi=dict(tasks="something"))))
    expected_new = tomli_w.dumps(dict(tool=dict(pixi=dict())))
    new = simplify_pyproject_toml(
        original_toml_string=old,
        pixi_environment="x",
        pixi_platform="x",
    )
    assert new == expected_new
