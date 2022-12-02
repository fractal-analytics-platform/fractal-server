FROM rancavil/slurm-node:19.05.5-1

COPY slurm.conf /etc/slurm-llnl/
RUN pip3 install clusterfutures==0.4
ADD fractal_server_local.tar.gz .
RUN pip3 install fractal-server

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
