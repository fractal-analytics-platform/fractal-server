FROM rancavil/slurm-master:19.05.5-1

RUN apt-get update
RUN apt-get install -y openssh-server
COPY slurm.conf /etc/slurm-llnl/
RUN useradd -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd
RUN useradd -m fractal -s /usr/bin/bash -d /home/fractal && echo "fractal:fractal" | chpasswd

EXPOSE 50097

ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
