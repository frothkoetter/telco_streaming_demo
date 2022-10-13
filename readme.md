# Telco Demo Streaming 
Stetup Guide 0.1 
Date Oct 11th 2022
Author: frothkoetter 

![](IMAGES/image1.png)


* Generate and Ingest Tower Events for GSM, UMTS and LTE
* Add the score form ML Model API 
* Route Events to a fraud topic 
* Convert raw events into JSON format and move to _json Topic
* Move raw data to Cloud Storage 
* Continues Query, aggregations and Join static with stream
* Visualize the data in a map
* Data Partitioning with with Iceberg and job management

Need DataHub Cluster: NIFI, KAFKA and FLINK and Kudu

![](IMAGES/image2.png)

Creation takes approximately 2 Hrs. - shutdown daily and startup time is approx 10 - 20 minutes and require startup of data generation after shutdown.

Data Services: CML, CDE, CDW and CDF 

Cost  awareness: 
Running these three DataHub Cluster for 8 hrs are: 120 USD approx

* Streaming Analytics (Light Duty): 37 USD
* Streams Messaging (Light Duty): 26 USD
* Flow Management (Light Duty): 14 USD
* Kudu Realtime Analyse : 43 USD
* CDW - Hive / Impala virtual Warehouse:
* CML - workspace:  

## Configure Nifi for DataGeneration and Routing
Figure: the full flow 
![](IMAGES/image3.png)

Upoad the FlowFile: HubDevDemo.json as template an import
(not using of FlowRegistry) 

Need to confiure processors 

S3Put - with Bucket location and AWS credentials 

![](IMAGES/image4.png)

Create in AWS the directory and check later that data is stored in the location.

![](IMAGES/image5.png)

## Telco Demo Tower event generator

Copy or clone this repo to your local machine and scp  to a NIFI worker node

`scp -r ./* frothkoetter@telco-demo-streaming-nifi-nifi0.se-sandb.a465-9q4k.cloudera.site:/home/frothkoetter
`

Install python lib on the worker node you connected: 

`pip3 install faker`

`pip3 install psutils`

`pip3 install numpy`

`pip3 install pandas`

Adjust gen.sh to your location and datasets of cell-towers. 

!/bin/sh

host=telco-demo-events-nifi2

while [ 1 ]
do
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'LTE' -ne 1000 -nd '1m' -f 'cell-tower.csv'
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'UMTS' -ne 400 -nd '1m' -f 'cell-tower.csv' 
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'GSM' -ne 600 -nd '1m' -f 'cell-tower.csv'
 sleep 50
done



Startup:  nohup ./gen.sh | nc telco-demo-events-nifi2 31888 & 

(telco-demo-events-nifi2 thats the local node you ssh to)!

Check events coming in Nifi on UI 




