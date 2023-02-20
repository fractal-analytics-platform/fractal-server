FROM rancavil/slurm-node:19.05.5-1

COPY slurm.conf /etc/slurm-llnl/
RUN pip3 install clusterfutures==0.5


ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
