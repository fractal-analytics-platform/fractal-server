import json
from pathlib import Path

from fractal_server.app.schemas.v2 import ValidProfileLocal
from fractal_server.app.schemas.v2 import ValidProfileSlurmSSH
from fractal_server.app.schemas.v2 import ValidProfileSlurmSudo
from fractal_server.app.schemas.v2 import ValidResourceLocal
from fractal_server.app.schemas.v2 import ValidResourceSlurmSSH
from fractal_server.app.schemas.v2 import ValidResourceSlurmSudo

base_dir = Path("docs/assets/resource_and_profile")

# LOCAL
with (base_dir / "resource_local.json").open("r") as f:
    resource = json.load(f)
    ValidResourceLocal(**resource)
with (base_dir / "profile_local.json").open("r") as f:
    profile = json.load(f)
    ValidProfileLocal(**profile)


# SLURM SUDO
with (base_dir / "resource_sudo.json").open("r") as f:
    resource = json.load(f)
    ValidResourceSlurmSudo(**resource)
with (base_dir / "profile_sudo.json").open("r") as f:
    profile = json.load(f)
    ValidProfileSlurmSudo(**profile)

# SLURM SSH
with (base_dir / "resource_ssh.json").open("r") as f:
    resource = json.load(f)
    ValidResourceSlurmSSH(**resource)
with (base_dir / "profile_ssh.json").open("r") as f:
    profile = json.load(f)
    ValidProfileSlurmSSH(**profile)
