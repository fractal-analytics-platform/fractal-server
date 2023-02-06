FROM rancavil/slurm-node:19.05.5-1

COPY slurm.conf /etc/slurm-llnl/
ADD fractal_server_local.tar.gz .
RUN pip3 install ./fractal-server[slurm]

RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
