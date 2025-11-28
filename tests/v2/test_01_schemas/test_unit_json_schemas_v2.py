import json
from pathlib import Path

import fractal_server.app.schemas.v2 as v2
from fractal_server.app.schemas.v2.manifest import Manifest


def test_ManifestV2_jsonschema():
    """
    Generate a JSON Schema from the ManifestV1 Pydantic model, and compare it
    with the one currently present in the repository.
    """
    json_schema_path = (
        Path(v2.__file__).parents[3] / "json_schemas/manifest_v2.json"
    )
    with json_schema_path.open("r") as f:
        current_schema = json.load(f)
    new_schema = Manifest.model_json_schema()
    assert new_schema == current_schema
