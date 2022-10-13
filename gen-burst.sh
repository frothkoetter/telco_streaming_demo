#!/bin/sh

host=telco-demo-events-nifi2

while [ 1 ] 
do 
 python3 gen-events.py -nc 1 -iso 'de' -state 'Germany' -r 'LTE' -ne 2000 -nd '1m' -f 'cell-tower.csv' 
 sleep 275 
done
