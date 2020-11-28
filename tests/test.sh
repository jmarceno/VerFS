#!/bin/bash

# /rsync -r --progress ~/test_data/* ~/fsteste/
echo
echo
echo De temp para VeratyFS
echo HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
rsync -r --progress /run/media/jmarceno/01D618D8C54EB2C0/temp/* ~/fsteste/

echo 
echo
echo
echo De temp para SSD ext4
echo HHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHHH
rsync -r --progress /run/media/jmarceno/01D618D8C54EB2C0/temp/* ~/metadata/
