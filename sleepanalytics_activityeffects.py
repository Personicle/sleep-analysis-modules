import numpy as np
import pandas as pd
from base_schema import *
from db_connection import *
from sqlalchemy import select
import pandas.io.sql as sqlio
import json
import sqlite3 as sq3
from sqlite3 import InterfaceError
import datetime
from personicle_functions import *
from utility_functions_sleepanalysis import *
from datetime import datetime, timedelta

query_events=query_events='select * from  personal_events '
events_stream= sqlio.read_sql_query(query_events,engine)

events_stream=events_overlap(events_stream)
events_stream['duration']=(events_stream['end_time'] - events_stream['start_time']) / pd.Timedelta(hours=1)
events_stream['end_date'] = pd.to_datetime(events_stream['end_time']).dt.date
events_stream['end_date']=events_stream['end_date'].astype(str)

#Matching events with sleep data

es1=events_stream[~(events_stream.event_name.isin(['Sleep']))] #Non-sleep
es2=events_stream[(events_stream.event_name.isin(['Sleep']))&(events_stream.duration<=15)] #sleep

es1['interval_start'] = es1['start_time'] + timedelta(hours=0)
es1['interval_end'] = es1['start_time'] + timedelta(hours=24)

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

#es1=Non-sleep, es2=sleep
es1='es1_name'
es2='es2_name'

qry = f"""
    select  
        {es1}.user_ID,
        {es1}.event_name activity_name,
        {es1}.start_time activity_start_time,
        {es1}.end_time activity_end_time,
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

pivot_sleep=sleep_event_matched_data.pivot_table(index=['user_id','sleep_duration'],columns=['activity_name'],values=['activity_duration'],aggfunc=np.sum).fillna(0).reset_index()



new_cols=[('{1} {0}'.format(*tup)) for tup in pivot_sleep.columns]

pivot_sleep.columns= new_cols

pivot_sleep.columns = pivot_sleep.columns.str.strip()

pivot_sleep.columns = pivot_sleep.columns. str. replace(' ','').str. replace('activity_duration','')

pivot_sleep.columns = map(str.lower,pivot_sleep.columns)


# for col in pivot_sleep.columns:
for col in pivot_sleep.columns:
    
    try:
        ddd = pivot_sleep[col].apply(lambda x : getCategory(x, dct_activity[col]))
        pivot_sleep[col] = ddd
        
    except TypeError:
        pass    
    
    except KeyError:
        pass
    
    except ValueError:
        pass



df_final=[]
for insight_activity in dct_activity.keys():
    
    
    df_final.append(copy.deepcopy(insights_generate(insight_activity,pivot_sleep,95)))
    
insights=pd.DataFrame(df_final).stack().apply(pd.Series)

try:
    
    insights['impact']=insights['impact'].astype(str)
    insights=insights[['user_id','life_aspect','impact','insighttext','timestampadded','expirydate','view']].copy()
    insights=insights[insights.insighttext.notnull()]

except KeyError:
    pass

insights.to_sql(name='insights', con=engine, index=False,if_exists='append')



