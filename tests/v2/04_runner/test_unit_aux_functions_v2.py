from fractal_server.app.runner.v2.runner_functions import deduplicate_list


def test_deduplicate_list_of_dicts():
    old = [dict(a=1), dict(b=2)]
    new = deduplicate_list(old)
    assert len(new) == 2
    old = [dict(a=1), dict(a=1), dict(b=2), dict(a=1)]
    new = deduplicate_list(old)
    assert len(new) == 2
