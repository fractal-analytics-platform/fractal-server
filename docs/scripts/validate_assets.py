import json
from pathlib import Path

from fractal_server.app.schemas.v2 import ValidProfileLocal
from fractal_server.app.schemas.v2 import ValidProfileSlurmSSH
from fractal_server.app.schemas.v2 import ValidProfileSlurmSudo
from fractal_server.app.schemas.v2 import ValidResourceLocal
from fractal_server.app.schemas.v2 import ValidResourceSlurmSSH
from fractal_server.app.schemas.v2 import ValidResourceSlurmSudo

assets_dir = Path("docs/assets/resource_and_profile")
output_path = Path("_docs_resource_and_profile_snippet.md")

with open(output_path, "w") as fd:

    def add_to_switch(title: str, json_item: dict) -> None:
        indent = "    "
        fd.write(f'=== "{title}"\n\n')
        fd.write(f"{indent}```json\n")
        fd.write(
            indent
            + json.dumps(json_item, indent=4).replace("\n", "\n" + indent)
            + "\n"
        )
        fd.write(f"{indent}```\n\n")

    fd.write("## Resource example\n\n")

    # Local
    with (assets_dir / "resource_local.json").open("r") as f:
        resource = json.load(f)
        ValidResourceLocal(**resource)
    add_to_switch("Local", resource)

    # SLURM sudo
    with (assets_dir / "resource_sudo.json").open("r") as f:
        resource = json.load(f)
        ValidResourceSlurmSudo(**resource)
    add_to_switch("SLURM sudo", resource)

    # SLURM ssh
    with (assets_dir / "resource_ssh.json").open("r") as f:
        resource = json.load(f)
        ValidResourceSlurmSSH(**resource)
    add_to_switch("SLURM ssh", resource)

    fd.write("## Profile example\n\n")

    with (assets_dir / "profile_local.json").open("r") as f:
        profile = json.load(f)
        ValidProfileLocal(**profile)
    add_to_switch("Local", profile)
    with (assets_dir / "profile_sudo.json").open("r") as f:
        profile = json.load(f)
        ValidProfileSlurmSudo(**profile)
    add_to_switch("SLURM sudo", profile)
    with (assets_dir / "profile_ssh.json").open("r") as f:
        profile = json.load(f)
        ValidProfileSlurmSSH(**profile)
    add_to_switch("SLURM ssh", profile)
