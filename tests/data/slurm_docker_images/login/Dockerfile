FROM rancavil/slurm-master:19.05.5-1

COPY slurm.conf /etc/slurm-llnl/
ADD sudoers /etc/sudoers

RUN useradd --uid 2000 -m fractal -s /usr/bin/bash -d /home/fractal && echo "fractal:fractal" | chpasswd
RUN useradd --uid 2001 -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd

RUN apt-get update && apt-get install -y openssh-server
RUN mkdir /var/run/sshd
# FIXME: is this EXPOSE needed?
EXPOSE 22

# We use our own version of https://github.com/rancavil/slurm-cluster/blob/main/master/docker-entrypoint.sh
COPY docker-entrypoint.sh /etc/slurm-llnl/

EXPOSE 50097


#ENTRYPOINT ["tail", "-f", "/dev/null"]
ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
