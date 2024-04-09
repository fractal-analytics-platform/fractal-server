import json
from pathlib import Path

import fractal_server.app.schemas.v1 as v1

new_schema = v1.manifest.ManifestV1.schema()
json_schema_path = (
    Path(v1.__file__).parents[3] / "json_schemas/manifest_v1.json"
)
with json_schema_path.open("w") as f:
    json.dump(new_schema, f, indent=4)
