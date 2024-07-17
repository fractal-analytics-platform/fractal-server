FROM ghcr.io/fractal-analytics-platform/ubuntu22-slurm:0.1

# Copy public SSH keys to container
COPY public_ssh_key ./
RUN mkdir /home/test01/.ssh
RUN mkdir /home/fractal/.ssh
RUN cat public_ssh_key >> /home/test01/.ssh/authorized_keys
RUN cat public_ssh_key >> /home/fractal/.ssh/authorized_keys
RUN chown test01:test01 /home/test01/.ssh/authorized_keys
RUN chown fractal:fractal /home/fractal/.ssh/authorized_keys
RUN chmod 600 /home/test01/.ssh/authorized_keys
RUN chmod 600 /home/fractal/.ssh/authorized_keys

# TODO (later): remove these lines, since it is not required that the slurm head has access to Python/fractal-server
RUN python3 -m pip install --upgrade pip setuptools wheel devtools
COPY tmp_requirements.txt ./
RUN python3 -m pip install -r tmp_requirements.txt

# TODO (later): remove these lines, since it is not required that the slurm head has access to Python/fractal-server
# These three lines cannot be cached
ADD fractal_server_local.tar.gz .
WORKDIR ./fractal-server
RUN python3 -m pip install -e .


EXPOSE 22 6817 6818

CMD ["tail", "-f"]
