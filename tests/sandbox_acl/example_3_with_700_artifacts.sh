#!/bin/bash

# Function to impersonate users
LINE="-----------------------------------------------------"
as_user() {
    echo "Run command \"$2\" as user $1";
    sudo su - $1 -c "$2";
    echo $LINE;
    echo;
}

# Function to remove test users and their home folders
cleanup_and_exit(){
    echo "Clean up and exit"
    # sudo rm -rf /tmp/acl_tests;
    sudo rm -r /home/testuser1;
    sudo rm -r /home/testuser2;
    sudo rm -r /home/testuser3;
    sudo userdel testuser1;
    sudo userdel testuser2;
    sudo userdel testuser3;
    echo $LINE;
    echo;
    exit 1;
}


# Cleanup environment
# Create new users, with homes and passwords
sudo useradd -m testuser1 -s /usr/bin/bash -d /home/testuser1
sudo useradd -m testuser2 -s /usr/bin/bash -d /home/testuser2
sudo useradd -m testuser3 -s /usr/bin/bash -d /home/testuser3
echo "testuser1:some-very-smart1-password-123" | sudo chpasswd
echo "testuser2:some-very-smart2-password-123" | sudo chpasswd
echo "testuser3:some-very-smart3-password-123" | sudo chpasswd
echo

# Check that the user creation was OK
as_user testuser1 id
as_user testuser2 id
as_user testuser3 id

# Check the umask of these users
as_user testuser1 "umask"

# NOTE: setting "umask 0000" here is useless, since it is then refreshed before
# the next command. This is achieved by combining the two commands, as in
as_user testuser1 "umask 0002 && umask"

# Create a folder
PARENT=/tmp/artifacts
JOBDIR=${PARENT}/jobdir
sudo rm -r $PARENT
as_user testuser1 "mkdir $PARENT"
as_user testuser1 "chmod 700 $PARENT"
as_user testuser1 "mkdir $JOBDIR"
as_user testuser1 "ls -lh $PARENT"
as_user testuser1 "chmod 700 $JOBDIR"
as_user testuser1 "ls -lh $PARENT"
echo

# Set ACL for $JOBDIR
as_user testuser1 "umask 0002 && setfacl -b $JOBDIR"
as_user testuser1 "umask 0002 && setfacl --default --modify user:testuser1:rwx,user:testuser2:rwx,group::---,other::--- $JOBDIR"
as_user testuser1 "umask 0002 && getfacl -p $JOBDIR"
echo

as_user testuser2 "umask 0002 && getfacl -p $JOBDIR"
exit

# Define some files in $JOBDIR
FILE1=${JOBDIR}/file-of-testuser1.txt
FILE2=${JOBDIR}/file-of-testuser2.txt
FILE3=${JOBDIR}/file-of-testuser3.txt

# Write a file as testuser1
as_user testuser1 "umask 0002 && echo This-is-written-by-testuser1 > $FILE1"
as_user testuser1 "umask 0002 && cat $FILE1"
as_user testuser1 "umask 0002 && getfacl -p $FILE1"
echo

# NOTE: output includes:
#   user:testuser1:rwx		#effective:rw-
#   user:testuser2:rwx		#effective:rw-
# which looks wrong

# Check (authorized) access by testuser2
as_user testuser2 "cat $FILE1"
echo

# Check (unauthorized) access by testuser3
as_user testuser3 "cat $FILE1"
echo

# Write a file as testuser2
as_user testuser2 "umask 0002 && echo This-is-written-by-testuser2 > $FILE2"
as_user testuser2 "umask 0002 && cat $FILE2"
as_user testuser2 "umask 0002 && getfacl -p $FILE2"
echo

# Check (authorized) access by testuser1
as_user testuser1 "cat $FILE2"
echo

# Check (unauthorized) access by testuser3
as_user testuser3 "cat $FILE2"
echo

# Write a file as testuser3 (unauthorized)
as_user testuser3 "umask 0002 && echo This-is-written-by-testuser3 > $FILE3"
echo

# Check permissions and ACL of $JOBDIR
as_user testuser1 "ls -lh /tmp | grep acl_tests"
as_user testuser1 "umask 0002 && getfacl -p $JOBDIR"
echo

# Exit
cleanup_and_exit
