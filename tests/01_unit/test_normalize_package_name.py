from fractal_server.tasks.utils import _normalize_package_name


def test_normalize_package_name():
    """
    Test based on the example in
    https://packaging.python.org/en/latest/specifications/name-normalization.
    """
    inputs = (
        "friendly-bard",
        "Friendly-Bard",
        "FRIENDLY-BARD",
        "friendly.bard",
        "friendly_bard",
        "friendly--bard",
        "FrIeNdLy-._.-bArD",
    )
    outputs = list(map(_normalize_package_name, inputs))
    assert len(set(outputs)) == 1
