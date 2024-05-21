FROM rancavil/slurm-master:19.05.5-1

COPY slurm.conf /etc/slurm-llnl/
ADD sudoers /etc/sudoers

RUN useradd --uid 2000 -m fractal -s /usr/bin/bash -d /home/fractal && echo "fractal:fractal" | chpasswd
RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

EXPOSE 50097
ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
