FROM rancavil/slurm-master:19.05.5-1

RUN sudo apt-get update

# TODO (later): remove these lines, since it is not required that the slurm head has access to Python/fractal-server
# Relink python3 (from 3.8 to 3.9)
RUN sudo rm -f /usr/bin/python3
RUN sudo ln -s /usr/bin/python3.9 /usr/bin/python3
RUN sudo apt-get install -y python3-pip python3.9-venv

COPY slurm.conf /etc/slurm-llnl/
ADD sudoers /etc/sudoers

RUN useradd --uid 2000 -m fractal -s /usr/bin/bash -d /home/fractal && echo "fractal:fractal" | chpasswd
RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

# SSH
RUN sudo apt-get install -y openssh-server
RUN mkdir /var/run/sshd

# Use custom version of docker-entrypoint.sh, which also starts sshd
COPY docker-entrypoint.sh /etc/slurm-llnl/

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

EXPOSE 22
EXPOSE 50097
ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
