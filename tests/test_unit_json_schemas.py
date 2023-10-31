import json
from pathlib import Path

import fractal_server.app.schemas as schemas
from fractal_server.app.schemas.manifest import ManifestV1


def test_ManifestV1_jsonschema():
    """
    Generate a JSON Schema from the ManifestV1 Pydantic model, and compare it
    with the one currently present in the repository.
    """
    json_schema_path = (
        Path(schemas.__file__).parent / "json_schemas/manifest.json"
    )
    with json_schema_path.open("r") as f:
        current_schema = json.load(f)
    new_schema = ManifestV1.schema()
    assert new_schema == current_schema
