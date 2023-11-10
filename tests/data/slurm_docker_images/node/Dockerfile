FROM rancavil/slurm-node:19.05.5-1

# Relink python3 (from 3.8 to 3.9)
RUN sudo rm -f /usr/bin/python3
RUN sudo ln -s /usr/bin/python3.9 /usr/bin/python3
RUN sudo apt-get install python3-pip

COPY slurm.conf /etc/slurm-llnl/
ADD fractal_server_local.tar.gz .
WORKDIR ./fractal-server

RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install fractal_server[slurm]
RUN pip3 install devtools

RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
