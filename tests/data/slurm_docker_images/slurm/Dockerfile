FROM ghcr.io/fractal-analytics-platform/ubuntu24-slurm-multipy:0.1

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

ARG PYTHON_PATH

RUN $PYTHON_PATH -m pip install pip --upgrade

# Only useful for python3.12: install setuptools
RUN $PYTHON_PATH -m pip install setuptools

# Install devtools
RUN $PYTHON_PATH -m pip install devtools

# Install fractal-server
ADD fractal_server_local.tar.gz .
WORKDIR ./fractal-server
RUN pwd
RUN ls -lh
RUN $PYTHON_PATH -m pip install -e .

EXPOSE 22 6817 6818

CMD ["tail", "-f"]
