Modules in this folder are consumed from the `update_db_data` function in
`fractal_server/__main__.py`. They must expose a `fix_db` function, and they
must be named according to `_slugify_version` (e.g. both for versions `1.4.3`
and `1.4.3a0` the module name is  `1_4_3.py`).
Modules corresponding to old versions should be moved to the `old` subfolder,
which is not included in the package wheel.
