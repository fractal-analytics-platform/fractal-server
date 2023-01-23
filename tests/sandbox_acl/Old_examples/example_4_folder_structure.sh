#!/bin/bash


# Function to impersonate users
run_as_user() {
    THIS_USERNAME=$1;
    THIS_COMMAND=$2;
    echo "${THIS_USERNAME}@${HOSTNAME}:~$ $THIS_COMMAND";
    sudo su - $THIS_USERNAME -c "$THIS_COMMAND";
    echo;
}

# Function to remove test users and their home folders
cleanup_and_exit(){
    echo "Clean up and exit"
    sudo rm -r /home/fractal;
    sudo rm -r /home/test01;
    sudo rm -r /home/test02;
    sudo userdel fractal;
    sudo userdel test01;
    sudo userdel test02;
    echo;
    exit 1;
}


### USERS #####################################################################

# Create new users, with homes and passwords
sudo useradd -m fractal -s /usr/bin/bash -d /home/fractal
sudo useradd -m test01 -s /usr/bin/bash -d /home/test01
sudo useradd -m test02 -s /usr/bin/bash -d /home/test02
echo "fractal:some-very-smart1-password-123" | sudo chpasswd
echo "test01:some-very-smart2-password-123" | sudo chpasswd
echo "test02:some-very-smart3-password-123" | sudo chpasswd
echo

### FOLDERS ###################################################################

# Define folder structure
FRACTAL_DIR=/tmp/fractal_server_folder
TASKS_DIR=${FRACTAL_DIR}/tasks
ENV_DIR=${TASKS_DIR}/myenv
ARTIFACTS_DIR=${FRACTAL_DIR}/artifacts
JOB_DIR=${ARTIFACTS_DIR}/workflow_0001_job_0001

# Create main folder afresh
sudo rm -r $FRACTAL_DIR
run_as_user fractal "mkdir $FRACTAL_DIR"
run_as_user fractal "chmod 711 $FRACTAL_DIR"

### TASKS #####################################################################

# Create folder and set permissions
run_as_user fractal "mkdir $TASKS_DIR"
run_as_user fractal "chmod 711 $TASKS_DIR"

# Create a task environment
run_as_user fractal "python3 -m venv $ENV_DIR"
run_as_user fractal "source ${ENV_DIR}/bin/activate && which pip3"
run_as_user fractal "source ${ENV_DIR}/bin/activate && pip3 install devtools"
run_as_user fractal "echo -e \"from devtools import debug\ndebug(1)\" > ${ENV_DIR}/task.py"

# Use task environment as another user
run_as_user test01 "${ENV_DIR}/bin/python ${ENV_DIR}/task.py"

### ARTIFACTS #################################################################

# Create folders and set permissions
run_as_user fractal "mkdir $ARTIFACTS_DIR"
run_as_user fractal "mkdir $JOB_DIR"
run_as_user fractal "chmod 711 $ARTIFACTS_DIR"
run_as_user fractal "chmod 700 $JOB_DIR"

# Set ACL for $JOBDIR
run_as_user fractal "setfacl -b $JOB_DIR"
ACL="user:fractal:rwx,user:test01:rwx,group::---,other::---"
run_as_user fractal "setfacl --recursive --modify $ACL $JOB_DIR"
run_as_user fractal "setfacl --default --recursive --modify $ACL $JOB_DIR"
run_as_user fractal "getfacl -p $JOB_DIR"
echo

# Define some files in $JOBDIR
FILE1=${JOB_DIR}/file-of-fractal.txt
FILE2=${JOB_DIR}/file-of-test01.txt
FILE3=${JOB_DIR}/file-of-test02.txt

# Write a file as fractal
run_as_user fractal "echo This-is-written-by-fractal > $FILE1"
run_as_user fractal "cat $FILE1"
run_as_user fractal "getfacl -p $FILE1"
echo

# Check (authorized) access by test01
run_as_user test01 "cat $FILE1"
echo

# Write a file as test01
run_as_user test01 "echo This-is-written-by-test01 > $FILE2"
run_as_user test01 "cat $FILE2"
run_as_user test01 "getfacl -p $FILE2"
echo

# Check (authorized) access by fractal
run_as_user fractal "cat $FILE2"
echo

# Check (unauthorized) access by test02
echo -e "WARNING: The next command should fail\n"
run_as_user test02 "cat $FILE1"
echo

# Check (unauthorized) access by test02
echo -e "WARNING: The next command should fail\n"
run_as_user test02 "cat $FILE2"
echo

# Write a file as test02 (unauthorized)
echo -e "WARNING: The next command should fail\n"
run_as_user test02 "echo This-is-written-by-test02 > $FILE3"
echo

run_as_user fractal "touch $ARTIFACTS_DIR/some-file-1.txt"

echo -e "WARNING: The next command should fail\n"
run_as_user test01 "touch $ARTIFACTS_DIR/some-file-2.txt"

echo -e "WARNING: The next command should fail\n"
run_as_user test02 "touch $ARTIFACTS_DIR/some-file-3.txt"

# Exit
cleanup_and_exit
