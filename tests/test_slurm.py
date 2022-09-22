from parsl.channels.ssh.ssh import SSHChannel
from devtools import debug


async def test_slurm(ssh_params):
    channel = SSHChannel(**ssh_params)

    retcode, stdout, stderr = channel.execute_wait("srun -h")

    debug(stderr)
    debug(stdout)
    assert retcode == 0
