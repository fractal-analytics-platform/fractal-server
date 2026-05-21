from fractal_server.runner.v2._worker_init import _get_worker_init_lines


def test_get_worker_init_lines():
    assert _get_worker_init_lines(worker_init="a\nb") == ["a", "b"]
    assert _get_worker_init_lines(worker_init=None) is None
