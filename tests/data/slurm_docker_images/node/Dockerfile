FROM rancavil/slurm-node:19.05.5-1

RUN apt-get update

# Relink python3 (from 3.8 to 3.9)
RUN sudo rm -f /usr/bin/python3
RUN sudo ln -s /usr/bin/python3.9 /usr/bin/python3
RUN sudo apt-get install -y python3-pip

COPY slurm.conf /etc/slurm-llnl/

RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

RUN pip3 install --upgrade pip setuptools wheel devtools
COPY tmp_requirements.txt ./
RUN pip3 install -r tmp_requirements.txt

# These three lines cannot be cached
ADD fractal_server_local.tar.gz .
WORKDIR ./fractal-server
RUN pip3 install -e .

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
