#!/bin/bash

# Function to impersonate users
run_as_user() {
    THIS_USERNAME=$1;
    THIS_COMMAND=$2;
    echo "${THIS_USERNAME}@${HOSTNAME}:~$ $THIS_COMMAND";
    sudo su - $THIS_USERNAME -c "$UMASK_PREFIX $THIS_COMMAND";
}

# Function to remove test users and their home folders
cleanup_and_exit(){
    echo "Clean up and exit"
    sudo rm -r /tmp/home-test01;
    sudo userdel test01;
    echo;
    exit 1;
}


# Create test01 user
sudo useradd -m test01 -s /usr/bin/bash -d /tmp/home-test01
echo "test01:some-very-smart2-password-123" | sudo chpasswd
echo

# Create server-side working folder
SERVERJOBDIR=/tmp/serverjobdir
sudo rm -fr $SERVERJOBDIR
mkdir $SERVERJOBDIR
chmod 755 $SERVERJOBDIR
echo

# Create user-side working folder
USERJOBDIR=/tmp/userjobdir
sudo rm -fr $USERJOBDIR
run_as_user test01 "mkdir $USERJOBDIR"
run_as_user test01 "chmod 700 $USERJOBDIR"
echo

# Make sure I/O test scripts are executable by all users (NOTE: all parent folders neew
cp script_dump_pickle.py $SERVERJOBDIR
cp script_load_pickle.py $SERVERJOBDIR
WRITESCRIPT=$SERVERJOBDIR/script_dump_pickle.py
READSCRIPT=$SERVERJOBDIR/script_load_pickle.py

# Server user writes to SERVERJOBDIR, user reads
$WRITESCRIPT $SERVERJOBDIR/x.pickle
chmod a+x $SERVERJOBDIR/x.pickle
run_as_user test01 "$READSCRIPT $SERVERJOBDIR/x.pickle"
echo

# User writes to USERJOBDIR, server can sudo-cat
run_as_user test01 "$WRITESCRIPT $USERJOBDIR/y.pickle"
sudo -u test01 cat $USERJOBDIR/y.pickle > $SERVERJOBDIR/y.pickle
$READSCRIPT $SERVERJOBDIR/y.pickle
echo

# Exit
cleanup_and_exit
