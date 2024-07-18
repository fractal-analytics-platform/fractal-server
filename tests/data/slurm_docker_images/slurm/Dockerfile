FROM ghcr.io/fractal-analytics-platform/ubuntu22-slurm-multipy:0.1

COPY slurm.conf /etc/slurm/

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

# FIXME: Python operations below should be repeated for all python versions?

# Update pip and install devtools
RUN python3.9 -m pip install --upgrade pip devtools
RUN python3.10 -m pip install --upgrade pip devtools
RUN python3.11 -m pip install --upgrade pip devtools

# Install requirements (useful for caching some layers when running locally)
COPY tmp_requirements.txt ./
ADD fractal_server_local.tar.gz .
RUN python3.9 -m pip install -r tmp_requirements.txt
RUN python3.10 -m pip install -r tmp_requirements.txt
RUN python3.11 -m pip install -r tmp_requirements.txt

# Install fractal-server
WORKDIR ./fractal-server
RUN python3.9 -m pip install -e .
RUN python3.10 -m pip install -e .
RUN python3.11 -m pip install -e .

EXPOSE 22 6817 6818

CMD ["tail", "-f"]
