#!/bin/bash

# /rsync -r --progress ~/test_data/* ~/fsteste/
echo
echo
echo De temp para VeratyFS
echo HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
time rsync -r --progress /run/media/jmarceno/01D618D8C54EB2C0/temp/* ~/fsteste/
fusermont -u ~/fsteste
cd ~/metadata
rm -r *

echo 
echo
echo
echo De temp para SSD ext4
echo HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
time rsync -r --progress /run/media/jmarceno/01D618D8C54EB2C0/temp/* ~/tmp/
cd ~/tmp
rm -r *
