#!/bin/bash

# This is a custom version of
# https://github.com/rancavil/slurm-cluster/blob/main/master/docker-entrypoint.sh,
# where we also start `sshd`.

export SLURM_CPUS_ON_NODE=$(cat /proc/cpuinfo | grep processor | wc -l)
sudo sed -i "s/REPLACE_IT/CPUs=${SLURM_CPUS_ON_NODE}/g" /etc/slurm-llnl/slurm.conf

sudo service munge start
sudo service slurmctld start

sudo /usr/sbin/sshd

tail -f /dev/null
