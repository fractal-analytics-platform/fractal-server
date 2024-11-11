import pytest

from fractal_server.tasks.v2.local.collect import (
    _customize_and_run_template as _customize_and_run_template_local,
)
from fractal_server.tasks.v2.ssh.collect import (
    _customize_and_run_template as _customize_and_run_template_ssh,
)


def test_customize_and_run_template_local():
    with pytest.raises(ValueError, match="must end with '.sh'"):
        _customize_and_run_template_local(
            template_filename="invalid",
            replacements={},
            script_dir="/somewhere",
            prefix="prefix",
        )

    with pytest.raises(FileNotFoundError):
        _customize_and_run_template_local(
            template_filename="invalid.sh",
            replacements={},
            script_dir="/somewhere",
            prefix="prefix",
        )


def test_customize_and_run_template_ssh():
    with pytest.raises(ValueError, match="must end with '.sh'"):
        _customize_and_run_template_ssh(
            template_filename="invalid",
            # Fake arguments
            replacements={},
            prefix="prefix",
            script_dir_local="/somewhere",
            fractal_ssh=None,
            script_dir_remote="/something",
        )

    with pytest.raises(FileNotFoundError):
        _customize_and_run_template_ssh(
            template_filename="invalid.sh",
            # Fake arguments
            replacements={},
            prefix="prefix",
            script_dir_local="/somewhere",
            fractal_ssh=None,
            script_dir_remote="/something",
        )
