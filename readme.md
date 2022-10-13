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

Configure the two putKafka procesors Broker and Topic
![](IMAGES/image7.png)

and user/pwd - use CDP workloaduser and pwd
![](IMAGES/image8.png)



## Telco Demo Tower event generator

Copy or clone this repo to your local machine and scp  to a NIFI worker node

`scp -r ./* frothkoetter@telco-demo-streaming-nifi-nifi0.se-sandb.a465-9q4k.cloudera.site:/home/frothkoetter
`

Install python lib on the worker node you connected: 

```
pip3 install faker
pip3 install psutils
pip3 install numpy
pip3 install pandas
```

Unzip cell-tower.csv 

` gunzip cell-tower.csv.gz`

Adjust gen.sh to your location and datasets of cell-towers. 

```
#!/bin/sh

host=telco-demo-events-nifi2

while [ 1 ]
do
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'LTE' -ne 1000 -nd '1m' -f 'cell-tower.csv'
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'UMTS' -ne 400 -nd '1m' -f 'cell-tower.csv' 
 python3 gen-events.py -nc 1 -iso 'be' -state 'Antwerp' -r 'GSM' -ne 600 -nd '1m' -f 'cell-tower.csv'
 sleep 50
done
```


Startup:  
`nohup ./gen.sh | nc telco-demo-events-nifi2 31888 & `


Check events coming in Nifi on UI 

![](IMAGES/image6.png)

## Prepare Messaging (Kafka) Cluster

Go to SMM and create topics (delete policy)

```
telco_tower_events_json
telco_tower_events_5min_json 
telco_tower_events_scored_json
```

Watch events coming into topics and recordconverter writing to the Kafka topics.

(check that the first character of the payload is NOT a [ : if yes then the NIFI RecordConverter JsonRecordWriter Service need to be configured correct) 

This is the correct JSON payload

## Configure SQL Streams Builder (SSB) 

Download CDP keytab from your profile and upload your keytab File to SSB 

![](IMAGES/image9.png)

Define the DataProvider for Kafka  

![](IMAGES/image10.png)

Next is to define the table, copy past the topic definition or try detect schema feature

```JSON
{
   "namespace": "example.avro",
   "type": "record",
   "name": "telco_events",
   "fields": [
      {"name": "event_id", "type": "string"},
      {"name": "imsi", "type": "string"},
      {"name": "start_time", "type": "string"},
      {"name": "up_time", "type": "string"},
      {"name": "cell", "type": "string"},
      {"name": "disconnect", "type": "string"},
      {"name": "drop_call", "type": "string"},
      {"name": "radio", "type": "string"},
      {"name": "mcc", "type": "string"},
      {"name": "net", "type": "string"},
      {"name": "area", "type": "string"},
      {"name": "long", "type": "string"},  
      {"name": "lat", "type": "string"},      
     {"name": "ingestion_dt", "type": "string"}   
   ] 
 }
```
You should see the column definition in the left window

![](IMAGES/image11.png)


## Run SQL Queries in SSB

### Query 1 - events grouped by network and dropped call over a tumbling window

```
select 
 radio,
 drop_call, 
 count(1) anz_events,
 TUMBLE_END( eventTimestamp, INTERVAL '15' MINUTE) AS window_end_timestamp 
from 
 telco_tower_events 
where
 eventTimestamp > cast( current_timestamp - INTERVAL '3' HOUR  as timestamp)
group by 
 TUMBLE( eventTimestamp, INTERVAL '15' MINUTE), radio, drop_call
```

Results


> `radio
> drop_call
> anz_events
> window_end_timestamp
> `








"GSM"
"True"
58732
"2022-10-05T10:45"
"GSM"
"True"
90323
"2022-10-05T11:00"
"UMTS"
"False"
102968
"2022-10-05T11:15"
"GSM"
"False"
85762
"2022-10-05T11:30"
"LTE"
"False"
58493
"2022-10-05T11:45"
"GSM"
"True"
89216
"2022-10-05T12:00"



### Query 2: IMSI with >= 10 Dropped Calls over a SLIDING window  

```
select cast( HOP_END( A.eventTimestamp, INTERVAL '5' MINUTE, INTERVAL '15' MINUTES)  as VARCHAR) AS window_end_timestamp ,
   A.imsi, count(* )as dropped_calls 
from  `ssb`.`ssb_default`.`telco_tower_events` A
where 
  drop_call = 'True'
group by 
 A.imsi ,
 HOP( A.eventTimestamp, INTERVAL '5' MINUTE, INTERVAL '15' MINUTE)
having count(* ) >= 10
```

Results
> window_end_timestamp
> imsi
> dropped_calls








"2022-10-10 07:25:00.000"
"643585455"
15
"2022-10-10 07:35:00.000"
"398120570"
37
"2022-10-10 07:45:00.000"
"579631667"
37
"2022-10-10 07:55:00.000"
"874820217"
25



## Store 5 min averages into Kafka Sink

If not done before create in SMM topic : telco_tower_events_5min_json 


!SSB Table create a schema collection : cleanup policy delete

Copy this schema:

```JSON
{
   "namespace": "example.avro",
   "type": "record",
   "name": "telco_events_5min_json",
   "fields": [
      {"name": "window_end_timestamp", "type": "string"},
      {"name": "radio", "type": "string"},
      {"name": "net", "type": "string"},
      {"name": "area", "type": "string"},
      {"name": "drop_call", "type": "string"},
      {"name": "cnt_events", "type": "string"}
   ] 
 }
```

Create a table telco_tower_events_5min_json

![](IMAGES/image12.png)



Enter in the console window:

```JSON
insert into telco_tower_events_5min_json 
select 
 cast( TUMBLE_END( eventTimestamp, INTERVAL '15' MINUTE) as VARCHAR) AS window_end_timestamp ,
 radio,net,area,drop_call, 
 cast(count(1) as VARCHAR) as cnt_events,
 cast(current_timestamp as TIMESTAMP)
from 
 telco_tower_events 
group by radio,net,area,drop_call, 
 TUMBLE( eventTimestamp, INTERVAL '15' MINUTE)
```

## Adding KUDU table to SSB
Allow Ranger Permissions on KUDU (add user to the allow)

Upload the mcc_mnc_international.parq to the S3 location.
i.e. 
`/goes-se-sandbox01/telco-demo/mcc_mnc_international/mcc_mnc_international.parq`

Go to Kudu Hue and create a table: 

```JSON 
drop table if exists mcc_mnc_international;
CREATE EXTERNAL TABLE mcc_mnc_international (
mcc INT,
mcc_int INT,
mnc INT,
mnc_int INT,
iso STRING,
country STRING,
country_code INT,
networks STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 's3a://goes-se-sandbox01/telco-demo/mcc_mnc_international';
```

```
select * from mcc_mnc_international;
```

```JSON
Drop table if exists mcc_mnc_international_kudu;
create table mcc_mnc_international_kudu (
mcc INT,
mnc INT,
iso STRING,
country STRING,
country_code INT,
networks STRING,
primary key( mcc, mnc))
stored as kudu;

insert into mcc_mnc_international_kudu
select mcc, mnc,iso, country, country_code, networks 
from mcc_mnc_international;

select * from mcc_mnc_international_kudu
```


### Query 3 - join two tables 
```
select A.mcc, B.mmc, A.mnc, B.net, B.radio, A.networks, B.event_id, B.up_time
from `telco_demo_kudu`.`default_database`.`default.mcc_mnc_international_kudu` A
, `ssb`.`ssb_default`.`telco_tower_events` B
where 
 A.mcc = cast( B.mmc as int) and
 A.mnc = cast( B.net as int) 
```

`mcc
mmc
mnc
net
radio
networks
event_id
up_time
`

















`204
"204"
8
"8"
"LTE"
"KPN Telecom B.V."
"efbb5832-486a-11ed-a431-06b826b14758"
"46"
`
`204
"204"
8
"8"
"LTE"
"KPN Telecom B.V."
"efc021fa-486a-11ed-a431-06b826b14758"
"167"`
`204
"204"
16
"16"
"LTE"
"T-Mobile B.V."
"efaa30f2-486a-11ed-a431-06b826b14758"
"138"`



### Query 4 - Join two tables over a tumbling window

```
select cast( TUMBLE_END( B.eventTimestamp, INTERVAL '15' MINUTE) as VARCHAR) AS window_end_timestamp ,
   A.networks, sum(cast( B.up_time as int) ) 
from `telco_demo_kudu`.`default_database`.`default.mcc_mnc_international_kudu` A
, `ssb`.`ssb_default`.`telco_tower_events` B
where 
 A.mcc = cast( B.mmc as int) and
 A.mnc = cast( B.net as int) 
group by 
 A.networks, 
 TUMBLE( B.eventTimestamp, INTERVAL '15' MINUTE)
```

`
window_end_timestamp
networks
EXPR$2
`


`"2022-10-10 07:15:00.000"
"KPN Telecom B.V."
30356`
`"2022-10-10 07:15:00.000"
"T-Mobile B.V."
8402`
`"2022-10-10 07:15:00.000"
"Vodafone Libertel"
31190`
`"2022-10-10 07:15:00.000"
"Tele2"
14387`
`"2022-10-10 07:30:00.000"
"Vodafone Libertel"
9532414`














