#!/usr/bin/env python
"""this python script will generate random alerts for a set of towers
you define some values for the run country, #of alerts, #of towers
CODE HIGH LEVEL FLOW
1. filter master input_file that contains all telco based on the chosen values
2. we get a list of random alerts for each tower
3. we append the alerts ot the towers
4. save result
"""

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
__version__ = "1.0.3"
__maintainer__ = "Philippe Lanckvrind"
__email__ = "planckvrind@cloudera.com"
__status__ = "Dev"

fake = Faker()
start_time = time.time()


def get_df_alert(number_events, event_alerts_tower_columns, net, mcc, area, radio_type, cell_id, long, lat, df_alert_coding_scheme):
    df_tmp = pd.DataFrame(index=range(number_events), columns=event_alerts_tower_columns)
    # df_tmp['cell_id'] = df_tmp['cell_id'].apply(lambda col: cell_id)
    df_tmp['cell_id'] = df_tmp['cell_id'].apply(lambda col: cell_id)

    df_tmp['event_id'] = df_tmp['event_id'].apply(lambda col: uuid.uuid1())
    df_tmp['start_time'] = df_tmp['start_time'].apply(
        lambda col: fake.date_time_between(start_date="-7d", end_date="now"))
    df_tmp['solved'] = df_tmp['solved'].apply(lambda col: bool(rn.getrandbits(1)))
    df_tmp['mcc'] = df_tmp['mcc'].apply(lambda col: mcc)
    df_tmp['net'] = df_tmp['net'].apply(lambda col: net)
    df_tmp['area'] = df_tmp['area'].apply(lambda col: area)
    df_tmp['radio_type'] = str(radio_type)
    df_tmp['long'] = df_tmp['long'].apply(lambda col: long)
    df_tmp['lat'] = df_tmp['lat'].apply(lambda col: lat)

    for index, row in df_tmp.iterrows():
        df_alert_1 = df_alert_coding_scheme.sample().reset_index()

        df_tmp.at[index, 'event_code'] = df_alert_1.loc[0, 'event_code']
        df_tmp.at[index, 'severity'] = df_alert_1.loc[0, 'severity']
        df_tmp.at[index, 'type'] = df_alert_1.loc[0, 'type']
        df_tmp.at[index, 'test_type'] = df_alert_1.loc[0, 'test_type']
        df_tmp.at[index, 'failure_cause'] = df_alert_1.loc[0, 'failure_cause']
    return df_tmp

def get_iterate_frame(df_portion, event_alerts_tower_columns,max_number_alert_event,df_alert_coding_scheme):
        # number_of_alerts is a global variable
    df_temp = pd.DataFrame(columns=event_alerts_tower_columns)   
    for index, row in df_portion.iterrows():
        df_temp = pd.concat([df_temp,get_df_alert(max_number_alert_event, event_alerts_tower_columns, row.net, row.mcc, row.area, row.radio, row.cell,row.long,row.lat,
                                df_alert_coding_scheme)])
    return df_temp

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--input-file",dest = "input_file", help="tower input file ")
    parser.add_argument("-iso", "--country-iso",dest = "address_country_code", help="country iso code be, us,... ")
    parser.add_argument("-state", "--country-state",dest = "selected_state", help="state  Texas, Brussels ... ")
    parser.add_argument("-city", "--country-city",dest = "address_city", help="city  Dallas, Paris ... ")
    parser.add_argument("-zip", "--zipcode",dest = "selected_postcode", help="tower postcode location ")
    parser.add_argument("-lac", "--location-area-code",dest = "selected_area", help="LAC is the unique number given to each location area within the network")
    parser.add_argument("-r", "--radio-type",dest = "radio_type", help="radio tower LTE, LTS, UMTS,GSM ")
    parser.add_argument("-ne", "--max-number-events",dest = "max_number_alert_event", help="max number of events to generate per tower")
    parser.add_argument("-nc", "--number-cores",dest = "num_procs", help="cores to parallelize the code")
    
    args = parser.parse_args()
    
    address_country_code = args.address_country_code
    # filter condition ADD ANTENA
    if args.selected_postcode is not None:
         filter_selection = args.selected_postcode
         df_col_name = 'address_postcode'
    elif args.selected_area is not None:
        df_col_name = 'area'
        filter_selection = args.selected_area
    elif args.address_city is not None:
        df_col_name = 'address_city'
        filter_selection = args.address_city
    else:
        df_col_name = 'address_state'
        filter_selection = args.address_state
    
    radio_type = args.radio_type
    max_number_alert_event = int(args.max_number_alert_event)
    input_file = args.input_file
    logical    = False
    df_results = []
    num_procs  = psutil.cpu_count(logical=logical)
    if args.num_procs is not None:
        num_procs = int(args.num_procs)

    input_file_alerts_coding_scheme = "DATA/event_qoe_types.csv"
    date_run = datetime.datetime.now().strftime("%d_%m_%Y")
    output_file = 'DATA/iso_{0}_{1}_{2}_n{3}_events_alerts_towers_lte_{4}.csv'.format(address_country_code, df_col_name,filter_selection.replace(" ", ""), max_number_alert_event, date_run)

    ### alert  coding scheme csv input
    alert_coding_scheme_columns = ['event_code', 'severity', 'type', 'test_type', 'failure_cause']
    df_alert_coding_scheme = pd.read_csv(input_file_alerts_coding_scheme, names=alert_coding_scheme_columns, header=1,
                                        sep=",", index_col=False)

    ### master DF alert structure ###
    event_alerts_tower_columns = ['cell_id', 'radio_type', 'event_id', 'event_code', 'severity', 'type', 'test_type',
                                'failure_cause', 'start_time', 'solved', 'mcc', 'net', 'area', 'lat', 'long']
    df_event_alerts_towers_lte = pd.DataFrame(columns=event_alerts_tower_columns)




    ### cell_towers csv  load, filter and clean up operation###

    towers_columns = ["cell", "mcc","net", "area", "long", "lat","radio", "address_city", "address_state","address_postcode","address_country","address_country_code","ingestion_dt"]
    df_towers = pd.read_csv(input_file, names=towers_columns, header=1, sep=",", index_col=False, low_memory=False)

    tower_columns_selection = ['radio', 'mcc', 'net', 'area', 'cell',"long","lat","address_postcode","address_country_code", "address_state", "address_city"]
    df_towers_col_selected = df_towers[tower_columns_selection]

    df_towers_mcc_radio_selected = df_towers_col_selected[(df_towers_col_selected[df_col_name].astype(str) == filter_selection) &
    (df_towers_col_selected['radio'].astype(str) == radio_type) & 
    (df_towers_col_selected['address_country_code'].astype(str) == address_country_code)]
    
    df_cell_towers = df_towers_mcc_radio_selected[df_towers_mcc_radio_selected.cell != '#VALUE!']
    df_cell_towers = df_cell_towers.reset_index(drop=True)
    print("---- number of towers selected : ", len(df_cell_towers.index))
    ### df alert data generator ###

    
    # parallel df processing
    splitted_df = np.array_split(df_cell_towers, num_procs)
    start = time.time()
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_procs) as executor:
        results = [ executor.submit(get_iterate_frame,df_portion=df, event_alerts_tower_columns=event_alerts_tower_columns,max_number_alert_event=max_number_alert_event,df_alert_coding_scheme=df_alert_coding_scheme ) for df in splitted_df ]
        for result in concurrent.futures.as_completed(results):
            try:
                df_results.append(result.result())
            except Exception as ex:
                print(str(ex))
                pass

    end = time.time()
    df_results = pd.concat(df_results)
    df_results['ingestion_dt'] = datetime.datetime.now()
    # save results
    df_results.to_csv(output_file, index=None)

    #df_event_alerts_towers_lte.to_csv(output_file, index=None)
    #print(df_event_alerts_towers_lte.head(2))
    print("---- number of towers : ", len(df_cell_towers.index))
    print("---- number of rows : ", len(df_results.index))
    print("--- %s seconds ---" % (time.time() - start_time))