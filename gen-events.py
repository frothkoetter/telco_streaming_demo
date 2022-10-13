#de!/usr/bin/env python
"""this python script will generate a set of calls to LTE towers 
for a dedicated mcc range defined by mcc_in | mcc_out
Sample of calls are defiled via sysarg[5]
It will be targeted to all cell towers
This scripts requires input files places in DATA FOLDER
"""

import os
import concurrent.futures
import psutil
import numpy as np
import pandas as pd
import random as rn
from faker import Faker
import uuid
import time
import datetime
import argparse

__author__ = "Philippe Lanckvrind, Nirchi David"
__copyright__ = "Copyright 2009 Cloudera"
__credits__ = ["Philippe Lanckvrind","Nirchi David"]
__license__ = "GPL"
__version__ = "1.0.5"
__maintainer__ = "Philippe Lanckvrind"
__email__ = "planckvrind@cloudera.com"
__status__ = "Beta_stage"

fake = Faker()
start_time = time.time()
event_tower_columns = ['event_id', 'imsi', 'start_time', 'up_time', 'cell', 'disconnect', 'drop_call','radio', 'mcc', 'net', 'area',"long","lat"]
input_file_customer_churn_imsi = "customers.csv"
customer_mobile_columns = ['customer_uuid', 'imsi']
df_customer_mobiles = pd.read_csv(input_file_customer_churn_imsi, names=customer_mobile_columns, header=1, sep=",", index_col=False)


def get_call_event_per_tower(radio,mcc,net,area,cell,long,lat,max_number_call_event,event_start_date):
    # empty dataframe before any other actions
    number_call_event = rn.randint(1,max_number_call_event)
    df_events = pd.DataFrame(index=range(number_call_event), columns=event_tower_columns)
    #randomize events construction
    df_events['radio'] = radio
    df_events['mcc'] = mcc
    df_events['net'] = net
    df_events['area'] = area
    df_events['cell'] = cell
    df_events['long'] = long
    df_events['lat'] = lat
    df_events['event_id'] = df_events['event_id'].apply(lambda col: uuid.uuid1())
    # add start_date as parameter default would be 7 days
    df_events['start_time'] = df_events['start_time'].apply(lambda col: fake.date_time_between(start_date=event_start_date, end_date="now"))
    df_events['up_time'] = df_events['up_time'].apply(lambda col: rn.randint(0, 180))
    df_events['disconnect'] = df_events['disconnect'].apply(lambda col: bool(rn.getrandbits(1)))
    df_events['drop_call'] = df_events['disconnect'].apply(lambda col: not col)
    df_events['imsi'] = df_events['imsi'].apply(lambda col: df_customer_mobiles.loc[rn.randrange(len(df_customer_mobiles)), 'imsi'])
    return df_events


def get_iterate_frame(df_portion, event_tower_columns, max_number_call_event, event_start_date ):
    df_temp = pd.DataFrame(columns=event_tower_columns)
    for index,row in df_portion.iterrows():
        radio = df_portion.at[index, 'radio']
        mcc = df_portion.at[index, 'mcc']
        area = df_portion.at[index, 'area']
        net = df_portion.at[index, 'net']
        cell = df_portion.at[index, 'cell']
        long = df_portion.at[index, 'long']
        lat = df_portion.at[index, 'lat']
        df_temp  = pd.concat([df_temp,get_call_event_per_tower(radio,mcc,net,area,cell,long,lat,max_number_call_event, event_start_date)])
    return df_temp

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--input-file",dest = "input_file", help="tower input file ")
    parser.add_argument("-iso", "--country-iso",dest = "address_country_code", help="country iso code be, us,... ")
    parser.add_argument("-state", "--country-state",dest = "address_state", help="state  Texas, Brussels ... ")
    parser.add_argument("-zip", "--ziptcode",dest = "selected_postcode", help="tower postcode location ")
    parser.add_argument("-lac", "--location-area-code",dest = "selected_area", help="LAC is the unique number given to each location area within the network")
    parser.add_argument("-r", "--radio-type",dest = "radio_type", help="radio tower LTE, LTS, UMTS,GSM ")
    parser.add_argument("-ne", "--max-number-events",dest = "max_number_call_event", help="max number of event to generate per tower")
    parser.add_argument("-nc", "--number-cores",dest = "num_procs", help="cores to parallelize the code")
    parser.add_argument("-nd", "--number-days",dest = "event_start_date", help=" expl -7d. between now and number of days in the past")
    args = parser.parse_args()
    
    address_country_code = args.address_country_code

    # filter condition 
    if args.selected_postcode is not None:
         filter_selection = args.selected_postcode
         df_col_name = 'address_postcode'
    elif args.selected_area is not None:
        df_col_name = 'area'
        filter_selection = args.selected_area
    else:
        df_col_name = 'address_state'
        filter_selection = args.address_state
    event_start_date = "-" + args.event_start_date
    radio_type = args.radio_type
    max_number_call_event = int(args.max_number_call_event)
    input_file = args.input_file
    logical    = False
    df_results = []
    num_procs  = psutil.cpu_count(logical=logical)
    if args.num_procs is not None:
        num_procs = int(args.num_procs)

    """ 
    num_procs = 5
    net = sys.argv[3]
    address_country_code = 'be'
    max_number_call_event = 500
    radio_type = 'LTE'
    selected_postcode = '1040' 
    input_file = './cell-towers.csv'
    #input_file = 'DATA/206_sample.csv'   
    """
    date_run = datetime.datetime.now().strftime("%d_%m_%Y")
    output_file = 'DATA/demo_call_events_towers_{0}.csv'.format(date_run)

#    output_file = 'DATA/iso_{0}_{1}_{2}_max_n{3}_radio{4}_call_events_towers_{5}.csv'.format(address_country_code, df_col_name,filter_selection.replace(" ", ""), max_number_call_event, radio_type, date_run)

    ### cell_towers csv  load, filter and clean up operation###
    towers_columns = ["cell", "mcc","net", "area", "long", "lat","radio", "address_county", "address_state","address_postcode","address_country_code"]
    df_towers = pd.read_csv(input_file, names=towers_columns, header=1, sep=",", index_col=False, low_memory=False)
    
    tower_columns_selection = ['radio', 'mcc', 'net', 'area', 'cell',"long","lat","address_postcode","address_country_code", "address_state"]
    df_towers_col_selected = df_towers[tower_columns_selection]
#    df_towers_mcc_radio_selected = df_towers_col_selected
    
    df_towers_mcc_radio_selected = df_towers_col_selected[(df_towers_col_selected[df_col_name].astype(str) == filter_selection) &
    (df_towers_col_selected['radio'].astype(str) == radio_type) & 
    (df_towers_col_selected['address_country_code'].astype(str) == address_country_code)]
    
    df_cell_towers = df_towers_mcc_radio_selected[df_towers_mcc_radio_selected.cell != '#VALUE!']
    df_cell_towers = df_cell_towers.reset_index(drop=True)
#    print("---- number of towers selected : ", len(df_cell_towers.index))
 
    # parallel df processing
    splitted_df = np.array_split(df_cell_towers, num_procs)
    start = time.time()
  
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_procs) as executor:
        results = [ executor.submit(get_iterate_frame,df_portion=df, event_tower_columns=event_tower_columns,max_number_call_event=max_number_call_event,event_start_date=event_start_date) for df in splitted_df ]
        for result in concurrent.futures.as_completed(results):
            try:
                df_results.append(result.result())
            except Exception as ex:
                print(str(ex))
                pass

    end = time.time()
    
    df_results = pd.concat(df_results)
    df_results['ingestion_dt'] = datetime.datetime.now()
#    print("---- time :", datetime.datetime.now())
    # save results
#    df_results.to_csv(output_file, index=None)
J = '\n'
s = J.join(df_results.astype(str).apply(lambda x: ','.join(x), axis=1))
print (s,end='\n')

# end of program
