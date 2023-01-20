FROM rancavil/slurm-node:19.05.5-1

RUN sudo apt-get install acl -y

COPY slurm.conf /etc/slurm-llnl/
ADD fractal_server_local.tar.gz .
RUN pip3 install ./fractal-server[slurm]
RUN pip3 install devtools

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
