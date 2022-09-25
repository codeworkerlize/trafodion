#!/bin/bash

source ~/.bashrc

#########################################

for comp in dtm rms dcs mgblty
do
  echo " "
  echo "Checking $comp status ..."
  http_trafcheck.py -i 15 -d 10 -c $comp
  ret=$?
done

echo " "
http_trafcheck.py 
echo " "

exit $ret
