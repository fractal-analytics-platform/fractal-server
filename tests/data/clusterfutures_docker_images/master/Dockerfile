FROM rancavil/slurm-master:19.05.5-1

RUN apt-get update
RUN sudo apt-get install python3-venv -y

RUN pip3 install clusterfutures==0.5
ENV CFUT_DIR=/tmp/CFUT_DIR

# COPY slurm_example.py .
# RUN sudo chown admin:admin slurm_example.py

COPY slurm.conf /etc/slurm-llnl/
ADD sudoers /etc/sudoers

RUN useradd --uid 2000 -m fractal -s /usr/bin/bash -d /home/fractal && echo "fractal:fractal" | chpasswd

EXPOSE 50097
ENTRYPOINT ["/etc/slurm-llnl/docker-entrypoint.sh"]
