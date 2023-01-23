#!/bin/bash

# Function to impersonate users
run_as_user() {
    THIS_USERNAME=$1;
    THIS_COMMAND=$2;
    echo "${THIS_USERNAME}@${HOSTNAME}:~$ $THIS_COMMAND";
    sudo su - $THIS_USERNAME -c "$THIS_COMMAND";
}

# Function to remove test users and their home folders
cleanup_and_exit(){
    echo "Clean up and exit"
    sudo rm -r /tmp/home-fractal;
    sudo rm -r /tmp/home-test01;
    sudo rm -r /tmp/home-test02;
    sudo userdel fractal;
    sudo userdel test01;
    sudo userdel test02;
    echo;
    exit 1;
}


### USERS #####################################################################

# Create new users, with homes and passwords
sudo useradd -m fractal -s /usr/bin/bash -d /tmp/home-fractal
sudo useradd -m test01 -s /usr/bin/bash -d /tmp/home-test01
sudo useradd -m test02 -s /usr/bin/bash -d /tmp/home-test02
echo "fractal:some-very-smart1-password-123" | sudo chpasswd
echo "test01:some-very-smart2-password-123" | sudo chpasswd
echo "test02:some-very-smart3-password-123" | sudo chpasswd
echo

### FOLDERS ###################################################################

# Define folder structure
FRACTAL_DIR=/tmp/fractal
ARTIFACTS_DIR=${FRACTAL_DIR}/artifacts
JOB_DIR=${ARTIFACTS_DIR}/workflow_0001_job_0001

# Create folders and set permissions
sudo rm -r $FRACTAL_DIR
run_as_user fractal "mkdir $FRACTAL_DIR"
run_as_user fractal "chmod 711 $FRACTAL_DIR"
run_as_user fractal "mkdir $ARTIFACTS_DIR"
run_as_user fractal "chmod 711 $ARTIFACTS_DIR"
run_as_user fractal "mkdir $JOB_DIR"
run_as_user fractal "chmod 700 $JOB_DIR"
echo

# Set ACL for $JOBDIR
run_as_user fractal "setfacl -b $JOB_DIR"
ACL="\
user:fractal:rwx,\
default:user:fractal:rwx,\
user:test01:rwx,\
default:user:test01:rwx,\
group::---,\
default:group::---,\
other::---,\
default:other::---,\
mask::rwx,\
default:mask::rwx\
"
run_as_user fractal "setfacl --recursive --modify $ACL $JOB_DIR"
run_as_user fractal "getfacl -p $JOB_DIR"
echo

### VERIFY EXPECTED BEHAVIOR ##################################################

# Write a file as fractal, read as test01
FILE1=${JOB_DIR}/file-of-fractal.txt
run_as_user fractal "echo This-file-was-written-by-fractal > $FILE1"
run_as_user test01 "cat $FILE1"
run_as_user fractal "getfacl -p $FILE1"
echo

# Write a file as test01, read as fractal
FILE2=${JOB_DIR}/file-of-test01.txt
run_as_user test01 "echo This-file-was-written-by-test01 > $FILE2"
run_as_user fractal "cat $FILE2"
run_as_user test01 "getfacl -p $FILE2"
echo

echo "--- WARNING: All following commands should fail ---"
echo

run_as_user test01 "touch $ARTIFACTS_DIR/some-file-2.txt"
run_as_user test02 "touch $ARTIFACTS_DIR/some-file-3.txt"
run_as_user test02 "touch $JOB_DIR/some-file.txt"
run_as_user test02 "cat $FILE1"
run_as_user test02 "cat $FILE2"

# Exit
cleanup_and_exit
