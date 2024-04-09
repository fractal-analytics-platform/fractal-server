import json
from pathlib import Path

import fractal_server.app.schemas.v2 as v2

new_schema = v2.manifest.ManifestV2.schema()
json_schema_path = (
    Path(v2.__file__).parent.parent.parent.parent
    / "json_schemas/manifest_v2.json"
)
with json_schema_path.open("w") as f:
    json.dump(new_schema, f, indent=4)
