FROM rancavil/slurm-node:19.05.5-1

COPY slurm.conf /etc/slurm-llnl/
ADD fractal_server_local.tar.gz .
RUN pip3 install ./fractal_server_local.tar.gz[slurm]

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
