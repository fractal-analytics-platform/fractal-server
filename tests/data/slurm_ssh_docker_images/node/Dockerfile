FROM rancavil/slurm-node:19.05.5-1

# Relink python3 (from 3.8 to 3.9)
RUN sudo rm -f /usr/bin/python3
RUN sudo ln -s /usr/bin/python3.9 /usr/bin/python3
RUN sudo apt-get install python3-pip

COPY slurm.conf /etc/slurm-llnl/

RUN pip3 install --upgrade pip setuptools wheel devtools

RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

COPY tmp_requirements.txt .
RUN pip3 install -r tmp_requirements.txt

ADD fractal_server_local.tar.gz .
WORKDIR ./fractal-server

# These three lines cannot be cached
RUN pip3 install -e .

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
