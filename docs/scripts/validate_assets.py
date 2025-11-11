import json
from pathlib import Path

from fractal_server.app.schemas.v2 import ValidProfileLocal
from fractal_server.app.schemas.v2 import ValidProfileSlurmSSH
from fractal_server.app.schemas.v2 import ValidProfileSlurmSudo
from fractal_server.app.schemas.v2 import ValidResourceLocal
from fractal_server.app.schemas.v2 import ValidResourceSlurmSSH
from fractal_server.app.schemas.v2 import ValidResourceSlurmSudo

assets_dir = Path("docs/assets/resource_and_profile")
output_path = Path("docs/assets/resource_and_profile/snippet.md")


def add_to_snippet(title: str, obj: dict, snippet: str) -> str:
    indent = "    "
    snippet += f'=== "{title}"\n\n'
    snippet += f"{indent}```json\n"
    snippet += (
        indent + json.dumps(obj, indent=4).replace("\n", "\n" + indent) + "\n"
    )
    snippet += f"{indent}```\n\n"
    return snippet


SNIPPET = ""

# Resource
SNIPPET += "## Resource example\n\n"
with (assets_dir / "resource_local.json").open("r") as f:
    resource = json.load(f)
    ValidResourceLocal(**resource)
SNIPPET = add_to_snippet("Local", resource, SNIPPET)
with (assets_dir / "resource_sudo.json").open("r") as f:
    resource = json.load(f)
    ValidResourceSlurmSudo(**resource)
SNIPPET = add_to_snippet("SLURM sudo", resource, SNIPPET)
with (assets_dir / "resource_ssh.json").open("r") as f:
    resource = json.load(f)
    ValidResourceSlurmSSH(**resource)
SNIPPET = add_to_snippet("SLURM ssh", resource, SNIPPET)


# Profile
SNIPPET += "## Profile example\n\n"
with (assets_dir / "profile_local.json").open("r") as f:
    profile = json.load(f)
    ValidProfileLocal(**profile)
SNIPPET = add_to_snippet("Local", profile, SNIPPET)
with (assets_dir / "profile_sudo.json").open("r") as f:
    profile = json.load(f)
    ValidProfileSlurmSudo(**profile)
SNIPPET = add_to_snippet("SLURM sudo", profile, SNIPPET)
with (assets_dir / "profile_ssh.json").open("r") as f:
    profile = json.load(f)
    ValidProfileSlurmSSH(**profile)
SNIPPET = add_to_snippet("SLURM ssh", profile, SNIPPET)


SNIPPET.strip("\n")

if not output_path.exists():
    with open(output_path, "w") as fd:
        fd.write(SNIPPET)
else:
    with open(output_path) as fd:
        current_snippet = fd.read()

    if current_snippet != SNIPPET:
        print(current_snippet)
        print(SNIPPET)
        raise ValueError("Snippets are different.")
