FROM rancavil/slurm-master:19.05.5-1

RUN apt-get update
RUN apt-get install -y openssh-server

RUN adduser test0
# RUN usermod -aG users test0
RUN echo "test0:pwd_test0" | chpasswd

RUN sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd

# RUN ssh-keygen -A
# RUN sudo service ssh start

RUN echo "#!/bin/bash" > /home/entrypoint.sh
RUN echo sudo ssh-keygen -A >> /home/entrypoint.sh
RUN echo sudo service ssh start >> /home/entrypoint.sh
RUN cat /etc/slurm-llnl/docker-entrypoint.sh >> /home/entrypoint.sh
RUN chmod +x /home/entrypoint.sh

# RUN mkdir /tmp/slurm_share
# RUN chown root:users -R /tmp/slurm_share
# RUN chmod -R a+rwx /tmp/slurm_share

ENTRYPOINT ["/home/entrypoint.sh"]
