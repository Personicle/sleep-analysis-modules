# -*- coding: utf-8 -*-
"""
Created on Wed Nov  2 15:05:53 2022

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

def d2d_scatterplot(cause_activity,temporal_time_hrs,effect_activity):
    
    data_stream_cause='select * from '+cause_activity
    data_stream_cause= sqlio.read_sql_query(data_stream_cause,engine)
    
    data_stream_effect='select * from '+effect_activity
    data_stream_effect= sqlio.read_sql_query(data_stream_effect,engine)
    
    data_stream_cause=timestamp_modify(data_stream_cause).copy()
    
    
    data_stream_effect=timestamp_modify(data_stream_effect).copy()
    
    data_stream_cause['end_date']= data_stream_cause['end_time'].dt.date
    
    data_stream_effect['end_date']= data_stream_effect['end_time'].dt.date
    
    
    df_agg_cause=data_stream_cause.groupby(['user_id','end_date','unit'])['value'].sum().reset_index()
    
    df_agg_effect=data_stream_effect.groupby(['user_id','end_date','unit'])['value'].mean().reset_index()
    #The aggregation has to be defined as per the activity type
    
    es1=df_agg_cause.copy()
    es2=df_agg_effect.copy()
    
    es1['interval_start'] = es1['end_date'] + timedelta(hours=0)
    es1['interval_end'] = es1['end_date'] + timedelta(hours=temporal_time_hrs)
    es1['event_name']=es1['unit'].map(eventname_unit)
    es2['event_name']=es2['unit'].map(eventname_unit)
    
    
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
    
    #es1=steps(cause), es2=hr(effect)
    es1='es1_name'
    es2='es2_name'
    
    qry = f"""
        select  
            {es1}.user_ID,
            {es1}.event_name cause_name,
            {es1}.interval_start cause_start_time,
            {es1}.interval_end cause_end_time,
             {es1}.value cause_value,
            {es2}.event_name effect_name,
            {es2}.end_date effect_end_date,
            {es2}.value effect_value
            
            
    
        from
            {es2} left join {es1} on
            (
            (
            {es1}.user_id={es2}.user_id
            )
            and
            
            (
            ({es2}.end_date between {es1}.interval_start and {es1}.interval_end)
            
            or 
        
            ({es2}.end_date between {es1}.interval_start and {es1}.interval_end)
            )
            )
    
            
            
            
            
        """
    
    matched_data = pd.read_sql_query(qry, conn)
    
    for col in ['cause_name','cause_start_time','cause_end_time','cause_value','effect_name','effect_end_date','effect_value']:
        matched_data[col].fillna(0,inplace=True)
    
    for f in ['cause_start_time','cause_end_time','effect_end_date']:
        matched_data[f] = pd.to_datetime(matched_data[f], infer_datetime_format=True)
    
    matched_data['activity_duration'] =matched_data['cause_end_time'] - matched_data['cause_start_time']
    matched_data['activity_duration']=matched_data['activity_duration']/np.timedelta64(1,'m')
    
    
    scatterplot_insights=matched_data[(matched_data.cause_name!=0)][['user_id','cause_value','effect_value','cause_name','effect_name']].copy()
    
    scatterplot_insights['cause_units']=list(eventname_unit.keys())[list(eventname_unit.values()).index(matched_data.cause_name.unique()[0])]
    scatterplot_insights['effect_units']=list(eventname_unit.keys())[list(eventname_unit.values()).index(matched_data.effect_name.unique()[0])]
    
    dic = {}
    
    
    for user_id in scatterplot_insights.user_id.unique():
        user_df = scatterplot_insights[scatterplot_insights['user_id']==user_id]
        l = []
        for i in range(user_df.shape[0]):
            row = user_df.iloc[i,:]
            #print(row) 
            l.append([row['cause_value'],row['effect_value']])
            
    
        dic[user_id] = {'XAxis' : {'Measure':scatterplot_insights.cause_name.unique()[0] , 'unit': "Total"+" "+eventname_unit[scatterplot_insights.cause_units.unique()[0]]+" "+"per day"}, 'YAxis': {
        'Measure': "heartrate",
        'unit': "bpm"
        }, 'data' : l}
     
    
    scatterplot_data = pd.DataFrame(dic.items())
    scatterplot_data.rename(columns={0:'user_id',1:'correlation_result'},inplace=True)
    
    scatterplot_data['analysis_id']= scatterplot_insights['cause_name'].map(activity_analysisid)   # Will be automated based on the analysis
    scatterplot_data['timestampadded']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
    scatterplot_data['view']='No'
    
    return scatterplot_data
    
