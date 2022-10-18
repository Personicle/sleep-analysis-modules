# -*- coding: utf-8 -*-
"""
Created on Mon Oct  3 12:19:18 2022

@author: nbadam
"""

import numpy as np
import pandas as pd
from base_schema import *
from db_connection import *
from sqlalchemy import select
import pandas.io.sql as sqlio
import json

import sqlite3 as sq3
from sqlite3 import InterfaceError

from personicle_functions import *
from utility_functions_sleepanalysis import *






def datastream_scatterplot(ds,temporal_time_hrs,effect_activity):
    
    query_events=query_events='select * from  personal_events '
    events_stream= sqlio.read_sql_query(query_events,engine)
    data_stream=datastream(ds).copy()
    
    data_stream=timestamp_modify(data_stream).copy()
    events_stream=events_overlap(events_stream)
    
    events_stream['duration']=(events_stream['end_time'] - events_stream['start_time']) / pd.Timedelta(hours=1)
    events_stream['end_date'] = pd.to_datetime(events_stream['end_time']).dt.date
    events_stream['end_date']=events_stream['end_date'].astype(str)
    
    
    # Getting list of dates that have high intense activities
    # to be removed to check Step count effect
    
    #Parameterize these as well for activity exclusion and should be based on event stream
    dates_tobe_excluded=events_stream[events_stream.event_name.isin(['Running','Biking','Strength training','Training','FunctionalStrengthTraining','Rowing','activity','Afternoon Ride','Cycling','Evening Run','Afternoon Run'])]
    
    
    events_stream=events_stream[~events_stream.end_date.isin(dates_tobe_excluded.end_date.unique())].copy()
    
    # Matching events with sleep data
    es1=data_stream.copy()
    #es2=events_stream[(events_stream.event_name.isin(['Sleep']))&(events_stream.duration<=15)] #sleep
    es2=events_stream[(events_stream.event_name.isin([effect_activity]))] #sleep
    
    es1['interval_start'] = es1['start_time'] + timedelta(hours=0)
    es1['interval_end'] = es1['start_time'] + timedelta(hours=temporal_time_hrs)
    es1['event_name']=es1['unit'].map(eventname_unit)
    
    
    try:
        es1['parameter2'] = es1['parameter2'].apply(lambda x: json.dumps(x))
        es2['parameter2'] = es2['parameter2'].apply(lambda x: json.dumps(x))
    
    except KeyError:
        pass
        
    # convert datetime to datetime string for sqlite
    for f in ['start_time', 'end_time', 'timestamp', 'interval_start', 'interval_end']:
        if f in es1.columns:
            es1[f] = es1[f].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S %z"))
        if f in es2.columns:
            es2[f] = es2[f].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S %z"))
            
    
    # convert the dfs to in-memory sqlite tables, join the tables, then read as df
    conn = sq3.connect(':memory:')
    # #write the tables
    try:
        es1.to_sql('es1_name', conn, index=False)
        es2.to_sql('es2_name', conn, index=False)
    except InterfaceError as e:
        print("Eventstream 1")
        print(es1.head())
        print(es1.dtypes)
    
        print("Eventstream 2")
        print(es2.head())
        print(es2.dtypes)
        raise e
    
    #es1=steps(datastream), es2=sleep
    es1='es1_name'
    es2='es2_name'
    
    qry = f"""
        select  
            {es1}.user_ID,
            {es1}.event_name activity_name,
            {es1}.start_time activity_start_time,
            {es1}.end_time activity_end_time,
             {es1}.value count,
            {es2}.event_name effect_event_name,
            {es2}.start_time sleep_start_time,
            {es2}.end_time sleep_end_time,
            {es2}.parameter2 {es2}_parameter2
            
            
    
        from
            {es2} left join {es1} on
            (
            (
            {es1}.user_id={es2}.user_id
            )
            and
            
            (
            ({es2}.start_time between {es1}.interval_start and {es1}.interval_end)
            
            or 
            
            ({es2}.end_time between {es1}.interval_start and {es1}.interval_end)
            )
            )
    
            
            
            
            
        """
    
    sleep_event_matched_data = pd.read_sql_query(qry, conn)
    
    
    for col in ['activity_name','activity_start_time','activity_end_time','sleep_start_time', 'sleep_end_time']:
        sleep_event_matched_data[col].fillna(0,inplace=True)
        
    
    for f in ['activity_start_time','activity_end_time','sleep_start_time', 'sleep_end_time']:
        sleep_event_matched_data[f] = pd.to_datetime(sleep_event_matched_data[f], infer_datetime_format=True)
    
    
    sleep_event_matched_data['activity_duration'] =sleep_event_matched_data['activity_end_time'] - sleep_event_matched_data['activity_start_time']
    sleep_event_matched_data['activity_duration']=sleep_event_matched_data['activity_duration']/np.timedelta64(1,'m')
    
    sleep_event_matched_data['sleep_duration'] =sleep_event_matched_data['sleep_end_time'] - sleep_event_matched_data['sleep_start_time']
    sleep_event_matched_data['sleep_duration']=sleep_event_matched_data['sleep_duration']/np.timedelta64(1,'h')
    
    
    sleep_event_matched_data=sleep_event_matched_data[(sleep_event_matched_data.activity_name!=0)].copy()
    
    
    
    #if sleep_event_matched_data.activity_name.unique()=='steps': #will change to list soon
    
    if sleep_event_matched_data.activity_name.unique() in (activity_cumulative):
        pivot_sleep=sleep_event_matched_data.pivot_table(index=['user_id','sleep_duration'],columns=['activity_name'],values=['activity_duration','count'],aggfunc=np.sum).fillna(0).reset_index()
      
    else:
        pivot_sleep=sleep_event_matched_data.pivot_table(index=['user_id','sleep_duration'],columns=['activity_name'],values=['activity_duration','count'],aggfunc=np.mean).fillna(0).reset_index()
    
        
        
    
    new_cols=[('{1} {0}'.format(*tup)) for tup in pivot_sleep.columns]
    
    # assign it to the dataframe (assuming you named it pivoted
    pivot_sleep.columns= new_cols
    
    # resort the index, so you get the columns in the order you specified
    #pivot_sleep.sort_index(axis='columns').head()
    
    
    pivot_sleep.columns = pivot_sleep.columns.str.strip()
    
    pivot_sleep.columns = pivot_sleep.columns. str. replace(' ','').str. replace('activity_duration','')
    
    pivot_sleep.rename(columns={ pivot_sleep.columns[-1]: "activity_value",pivot_sleep.columns[-2]: "activity_duration" },inplace=True)
    
    
    pivot_sleep['activity_name']=sleep_event_matched_data.activity_name.unique()[0]
    pivot_sleep['activity_units']=list(eventname_unit.keys())[list(eventname_unit.values()).index(sleep_event_matched_data.activity_name.unique()[0])]
    
    pivot_sleep.columns = map(str.lower,pivot_sleep.columns)
    
    
    scatterplot_insights=pivot_sleep.copy()
    
    display(scatterplot_insights.head())
    
    dic = {}
    
    
    for user_id in scatterplot_insights.user_id.unique():
        user_df = scatterplot_insights[scatterplot_insights['user_id']==user_id]
        l = []
        for i in range(user_df.shape[0]):
            row = user_df.iloc[i,:]
            #print(row) 
            l.append([row['activity_value'],row['sleep_duration']])
            
    
        dic[user_id] = {'XAxis' : {'Measure':scatterplot_insights.activity_name.unique()[0] , 'unit': "Total"+" "+eventname_unit[scatterplot_insights.activity_units.unique()[0]]+" "+"per day"}, 'YAxis': {
        'Measure': "Sleep",
        'unit': "hours"
        }, 'data' : l}
     
    
    scatterplot_data = pd.DataFrame(dic.items())
    scatterplot_data.rename(columns={0:'user_id',1:'correlation_result'},inplace=True)
    
    scatterplot_data['analysis_id']= scatterplot_insights['activity_name'].map(activity_analysisid)   # Will be automated based on the analysis
    scatterplot_data['timestampadded']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
    scatterplot_data['view']='No'
    
    return scatterplot_data
    
