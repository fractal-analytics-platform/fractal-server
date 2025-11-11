import json
from pathlib import Path

import mkdocs_gen_files

from fractal_server.app.schemas.v2 import ValidProfileLocal
from fractal_server.app.schemas.v2 import ValidProfileSlurmSSH
from fractal_server.app.schemas.v2 import ValidProfileSlurmSudo
from fractal_server.app.schemas.v2 import ValidResourceLocal
from fractal_server.app.schemas.v2 import ValidResourceSlurmSSH
from fractal_server.app.schemas.v2 import ValidResourceSlurmSudo

assets_dir = Path("docs/assets/resource_and_profile")


def add_to_switch(title: str, json_item: dict, fd) -> None:
    indent = "    "
    fd.write(f'=== "{title}"\n\n')
    fd.write(f"{indent}```json\n")
    fd.write(
        indent
        + json.dumps(json_item, indent=4).replace("\n", "\n" + indent)
        + "\n"
    )
    fd.write(f"{indent}```\n\n")


with mkdocs_gen_files.open("resource_switcher.md", "w") as fd:
    # Local
    with (assets_dir / "resource_local.json").open("r") as f:
        resource = json.load(f)
        ValidResourceLocal(**resource)
    add_to_switch("Local", resource, fd)

    # SLURM sudo
    with (assets_dir / "resource_sudo.json").open("r") as f:
        resource = json.load(f)
        ValidResourceSlurmSudo(**resource)
    add_to_switch("SLURM sudo", resource, fd)

    # SLURM ssh
    with (assets_dir / "resource_ssh.json").open("r") as f:
        resource = json.load(f)
        ValidResourceSlurmSSH(**resource)
    add_to_switch("SLURM ssh", resource, fd)


with mkdocs_gen_files.open("profile_switcher.md", "w") as fd:
    with (assets_dir / "profile_local.json").open("r") as f:
        profile = json.load(f)
        ValidProfileLocal(**profile)
    add_to_switch("Local", profile, fd)
    with (assets_dir / "profile_sudo.json").open("r") as f:
        profile = json.load(f)
        ValidProfileSlurmSudo(**profile)
    add_to_switch("SLURM sudo", profile, fd)
    with (assets_dir / "profile_ssh.json").open("r") as f:
        profile = json.load(f)
        ValidProfileSlurmSSH(**profile)
    add_to_switch("SLURM ssh", profile, fd)
