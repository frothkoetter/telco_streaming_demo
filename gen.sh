#!/bin/sh

host=telco-demo-events-nifi2

while [ 1 ] 
do 
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'LTE' -ne 5000 -nd '1m' -f 'cell-tower.csv' 
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'UMTS' -ne 1000 -nd '1m' -f 'cell-tower.csv' 
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'GSM' -ne 500 -nd '1m' -f 'cell-tower.csv' 
 sleep 50 
done
