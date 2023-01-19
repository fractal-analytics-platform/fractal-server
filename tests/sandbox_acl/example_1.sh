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
    echo "Clean up test users and exit"
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

# Exit
cleanup_and_exit




DIR=/tmp/acl_dir
sudo rm -r $DIR
AFILE=${DIR}/logA.txt
BFILE=${DIR}/logB.txt
as_user testuser1 "mkdir $DIR"
as_user testuser1 "ls -lh /tmp | grep acl"
echo


as_user testuser1 "setfacl -b $DIR"
as_user testuser1 "setfacl --modify user:testuser1:rwx,user:testuser2:rwx,group::---,other::--- $DIR"
as_user testuser1 "setfacl --default --modify user:testuser1:rwx,user:testuser2:rwx,group::---,other::--- $DIR"
as_user testuser1 "getfacl -p $DIR"
echo

# as_user testuser1 "mkdir $DIR/subdir"
# as_user testuser1 "getfacl -p $DIR/subdir"
# echo

as_user testuser1 "echo 123 > $AFILE"
as_user testuser1 "cat $AFILE"
as_user testuser1 "getfacl -p $AFILE"
echo

as_user testuser2 "cat $AFILE"
echo

as_user testuser2 "echo 456 > $BFILE"
echo

#as_user testuser2 "touch $BFILE"
#as_user testuser2 "echo 123 >> $BFILE"
#as_user testuser2 "cat $BFILE"
#as_user testuser2 "getfacl -p $BFILE"
echo

umask $UMASK


#as_user testuser1 "rm -rf $DIR"

#$U1 getfacl -p $DIR
#echo

#$U1 setfacl -b $DIR
#$U1 setfacl --default --modify user:testuser1:rwx,user:testuser2:rwx $DIR
#echo
#$U1 getfacl -p $DIR
#echo
#$U1 setfacl --modify u:testuser2:rwx,u:testuser1:rwx $DIR

#$U1 echo "This is a log by testuser1" > $FILE
#$U1 getfacl -p $FILE

#$U1 echo "User testuser1 can write on this file" >> $FILE
#$U3 echo "User testuser3 cannot write on this file" >> $FILE

#$U1 rm -rf $DIR


#$ chmod 700 /tmp/w5

#$ ls -la /tmp/w5

#$ setfacl -m u:testuser2:rwx,u:testuser1:rwx /tmp/w5

#$ getfacl /tmp/w5
