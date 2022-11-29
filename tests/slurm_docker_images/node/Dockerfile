FROM rancavil/slurm-node:19.05.5-1

RUN pip3 install clusterfutures==0.4
RUN pip3 install https://github.com/fractal-analytics-platform/fractal-server/releases/download/1.0.0b1/fractal_server-1.0.0b1-py3-none-any.whl
COPY slurm.conf /etc/slurm-llnl/

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
