import json
from pathlib import Path

import fractal_server.app.schemas.v1 as v1
from fractal_server.app.schemas.v1.manifest import ManifestV1


def test_ManifestV1_jsonschema():
    """
    Generate a JSON Schema from the ManifestV1 Pydantic model, and compare it
    with the one currently present in the repository.
    """
    json_schema_path = (
        Path(v1.__file__).parents[3] / "json_schemas/manifest_v1.json"
    )
    with json_schema_path.open("r") as f:
        current_schema = json.load(f)
    new_schema = ManifestV1.model_json_schema()
    assert new_schema == current_schema
