import pytest

from fractal_server.tasks.v2.collection_local import (
    _customize_and_run_template as _customize_and_run_template_local,
)
from fractal_server.tasks.v2.collection_ssh import (
    _customize_and_run_template as _customize_and_run_template_ssh,
)


def test_customize_and_run_template_local():
    with pytest.raises(ValueError, match="must end with '.sh'"):
        _customize_and_run_template_local(
            template_filename="invalid",
            replacements={},
            script_dir="/somewhere",
            prefix="prefix",
            logger_name="logger",
        )

    with pytest.raises(FileNotFoundError):
        _customize_and_run_template_local(
            template_filename="invalid.sh",
            replacements={},
            script_dir="/somewhere",
            prefix="prefix",
            logger_name="logger",
        )


def test_customize_and_run_template_ssh():
    with pytest.raises(ValueError, match="must end with '.sh'"):
        _customize_and_run_template_ssh(
            template_filename="invalid",
            # Fake arguments
            replacements={},
            prefix="prefix",
            logger_name="logger",
            script_dir="/somewhere",
            fractal_ssh=None,
            tasks_base_dir="/something",
        )

    with pytest.raises(FileNotFoundError):
        _customize_and_run_template_ssh(
            template_filename="invalid.sh",
            # Fake arguments
            replacements={},
            prefix="prefix",
            script_dir="/somewhere",
            logger_name="logger",
            fractal_ssh=None,
            tasks_base_dir="/something",
        )
