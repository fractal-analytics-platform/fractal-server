"""
Fakes to run `SlurmRestRunner` against the dockerized SLURM cluster, which
has no `slurmrestd`: only the REST client and the token provider are
replaced, the rest of the runner is real.
"""

from uuid import uuid4

from fractal_server.runner.executors.slurm_rest import runner as runner_module
from fractal_server.ssh._fabric import FractalSSH

# SLURM user of the fake token: different from the SSH user (as with SSO
# tokens and a shared SSH account), and known in the container
SLURM_USERNAME = "root"


class SSHFakeRestClient:
    """
    Run REST requests through `sbatch`, `squeue` and `scancel` over SSH.

    The submitted `script` (with its embedded input files) is run as is, while
    the JSON `job` description is not used (`sbatch` reads the `#SBATCH`
    lines instead).
    """

    base_url = "fake://slurmrestd"

    def __init__(self, fractal_ssh: FractalSSH):
        self.fractal_ssh = fractal_ssh
        self.submitted: list[dict] = []
        self.client_kwargs: dict = {}

    def ping(self) -> None:
        pass

    def submit_job(self, request_body: dict) -> int:
        self.submitted.append(request_body)
        workdir = request_body["job"]["current_working_directory"]
        path = f"{workdir}/rest-{uuid4().hex}.sh"
        self.fractal_ssh.write_remote_file(
            path=path, content=request_body["script"]
        )
        stdout = self.fractal_ssh.run_command(cmd=f"sbatch --parsable {path}")
        return int(stdout.strip())

    def get_job_states(self) -> dict[str, str]:
        stdout = self.fractal_ssh.run_command(
            cmd="squeue --noheader --format='%i %T' --states=all"
        )
        return dict(line.split() for line in stdout.splitlines())

    def cancel_job(self, job_id: str) -> None:
        self.fractal_ssh.run_command(cmd=f"scancel {job_id}")


class FakeTokenProvider:
    slurm_username = SLURM_USERNAME

    def get_token(self, force_refresh: bool = False) -> str:
        return "x"


def use_fake_slurm_rest(
    monkeypatch, fractal_ssh: FractalSSH
) -> SSHFakeRestClient:
    """
    Make every `SlurmRestRunner` use a `SSHFakeRestClient` (returned) and a
    `FakeTokenProvider`.
    """
    fake = SSHFakeRestClient(fractal_ssh)

    def _client(**kwargs) -> SSHFakeRestClient:
        fake.client_kwargs = kwargs
        return fake

    monkeypatch.setattr(runner_module, "SlurmRestClient", _client)
    monkeypatch.setattr(
        runner_module, "get_token_provider", lambda **_: FakeTokenProvider()
    )
    return fake
