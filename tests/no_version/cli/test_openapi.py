from fractal_server.cli._openapi import save_openapi


def test_save_openapi(tmp_path):
    dest = tmp_path / "openapi.json"
    save_openapi(dest=dest.as_posix())
    assert dest.exists()
