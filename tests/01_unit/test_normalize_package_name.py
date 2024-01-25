from fractal_server.tasks.naming import _normalize_package_name


def test_normalize_package_name():
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
