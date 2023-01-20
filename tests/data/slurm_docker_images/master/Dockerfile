FROM rancavil/slurm-master:19.05.5-1

RUN apt-get update
RUN sudo apt-get install python3-venv -y
RUN sudo apt-get install acl -y
RUN sudo apt-get install glibc-source #needed to use os.getlogin()

COPY slurm.conf /etc/slurm-llnl/
ADD sudoers /etc/sudoers
RUN useradd -m fractal -s /usr/bin/bash -d /home/fractal && echo "fractal:fractal" | chpasswd
RUN useradd -m test01 -s /usr/bin/bash -d /home/test01 && echo "test01:test01" | chpasswd
RUN useradd -m test02 -s /usr/bin/bash -d /home/test02 && echo "test02:test02" | chpasswd

EXPOSE 50097
ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
