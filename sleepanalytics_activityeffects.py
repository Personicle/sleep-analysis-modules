import numpy as np
import pandas as pd
from base_schema import *
from db_connection import *
from sqlalchemy import select
import pandas.io.sql as sqlio
from datetime import datetime
from datetime import date
import calendar
import json
import math
import datetime
from time import gmtime, strftime
from datetime import timedelta, date

from scipy import stats
from math import sqrt
from scipy.stats import t

from datetime import datetime as dt, timedelta

from dct_activity import dct_activity



import sqlite3 as sq3
from sqlite3 import InterfaceError
import os
import sys

from psycopg2.extensions import register_adapter, AsIs


query_events=query_events='select * from  personal_events '
events_df= sqlio.read_sql_query(query_events,engine)

#Match event effects on sleep

match_events=events_df[(events_df.event_name!='')&(events_df.event_name.notnull())]

#Handling overlapping events


data=match_events.sort_values(by=['start_time']).copy()

data["end_time_lag"] = data['end_time'].shift()
def merge(x):

    if (x.start_time <=x.end_time_lag):
        x.end_time = max(x.end_time, x.end_time_lag)

    return x

res = pd.DataFrame([])
for k,grp in (data.groupby(['user_id','event_name','source'],as_index=False)):

    res = pd.concat([grp.apply(merge, axis=1).groupby(['user_id','event_name','source'],as_index=False).apply(lambda e: e.assign(
        grp=lambda d: (
            ~(d["start_time"] <= (d["end_time"].shift()))
            ).cumsum())), res])
    
    

data = res.groupby(['user_id','event_name','grp'],as_index=False).agg({"start_time": "min", "end_time": "max"
                                                                                    ,'parameters': list,
                                                                                    'source':';'.join})
 
def param_append(list1,total_duration):
        dct={}
        totalcaloriesburned=0
        for k,v in enumerate(list1):
            #v=v.replace("'",'"')
            #v=json.loads(v)
            dct['param'+str(k)]=v
            
            if 'caloriesBurned' in v:
                totalcaloriesburned+=v['caloriesBurned']
    
        if totalcaloriesburned!=0:
            dct['totalcaloriesburned']=totalcaloriesburned
         
        dct['duration']=np.int(total_duration*60000)
    
        return dct
                                                                                      
data.drop(columns='grp',inplace=True)


data['duration_minutes'] = data['end_time'] - data['start_time']
data['duration_minutes']=data['duration_minutes']/np.timedelta64(1,'m')


lst=[]
for i in range(data.shape[0]):
    temp=param_append(data.iloc[i,data.columns.get_loc('parameters')],data.iloc[i,data.columns.get_loc('duration_minutes')])
    lst.append(temp)


data['parameter2']=lst


data['parameter2'] = data['parameter2'].apply(lambda x: json.dumps(x))


data=data[['user_id', 'start_time', 'end_time', 'event_name', 'source','parameter2']].copy()

data['duration']=(data['end_time'] - data['start_time']) / pd.Timedelta(hours=1)

es1=data[data.event_name!='Sleep'] #non-sleep

es2=data[(data.event_name=='Sleep')&(data.duration<=15)] #sleep


es1['interval_start'] = es1['start_time'] + timedelta(hours=0)
es1['interval_end'] = es1['start_time'] + timedelta(hours=24)

es1['parameter2'] = es1['parameter2'].apply(lambda x: json.dumps(x))
es2['parameter2'] = es2['parameter2'].apply(lambda x: json.dumps(x))

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


#es1=activity, es2=sleep

qry = """
    select  
        {es1}.user_ID,
        {es1}.event_name {es1}_event_name,
        {es1}.start_time {es1}_start_time,
        {es1}.end_time {es1}_end_time,
        {es1}.parameter2 {es1}_parameter2,
        {es2}.event_name {es2}_event_name,
        {es2}.start_time {es2}_start_time,
        {es2}.end_time {es2}_end_time,
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

        
        
        
        
    """.format(es1='es1_name', es2='es2_name')
result_df = pd.read_sql_query(qry, conn)
#conn.close()



for col in ['es1_name_event_name','es1_name_start_time','es1_name_end_time','es2_name_start_time', 'es2_name_end_time']:
    result_df[col].fillna(0,inplace=True)
    
for f in ['es1_name_start_time', 'es1_name_end_time', 'es2_name_start_time', 'es2_name_end_time']:
    result_df[f] = pd.to_datetime(result_df[f], infer_datetime_format=True)


result_df['activity_duration'] =result_df['es1_name_end_time'] - result_df['es1_name_start_time']
result_df['activity_duration']=result_df['activity_duration']/np.timedelta64(1,'m')

result_df['sleep_duration'] =result_df['es2_name_end_time'] - result_df['es2_name_start_time']
result_df['sleep_duration']=result_df['sleep_duration']/np.timedelta64(1,'h')

pivot_sleep=result_df.pivot_table(index=['user_id','sleep_duration'],columns=['es1_name_event_name'],values=['activity_duration'],aggfunc=np.sum).fillna(0).reset_index()


new_cols=[('{1} {0}'.format(*tup)) for tup in pivot_sleep.columns]

# assign it to the dataframe (assuming you named it pivoted
pivot_sleep.columns= new_cols

# resort the index, so you get the columns in the order you specified
pivot_sleep.sort_index(axis='columns').head()


pivot_sleep.columns = pivot_sleep.columns.str.strip()

pivot_sleep.columns = pivot_sleep.columns. str. replace(' ','').str. replace('activity_duration','')
pivot_sleep.columns = map(str.lower,pivot_sleep.columns)


cols = pivot_sleep.columns[2:]
pivot_sleep[cols] =pivot_sleep[cols].round(0)


##assigning categories to activity levels

def getCategory(num, dct):
    
    for key in dct.keys():
        if dct[key][0] <= num <= dct[key][1]:
            return key

        
        
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


#Generating insights for various users across various activoties stated in the dictionary
def insights_generate(insight_activity):
    current_activity=insight_activity #change here


    stats_sleep=[]
    stats_sleep = pivot_sleep.groupby(['user_id',current_activity])['sleep_duration'].agg(['mean', 'count', 'std'])

    ci95_hi = []
    ci95_lo = []

    for i in stats_sleep.index:
        m, c, s = stats_sleep.loc[i]
        ci95_hi.append(m + 1.96*s/math.sqrt(c))
        ci95_lo.append(m - 1.96*s/math.sqrt(c))

    stats_sleep['ci95_hi'] = ci95_hi
    stats_sleep['ci95_lo'] = ci95_lo
    #stats_sleep['event_name']='sleep'


    alert=stats_sleep.reset_index().copy()


    alert.rename(columns={current_activity:'event_summary'},inplace=True)
    




    temp2={}
    ds = []

    for user_id in alert.user_id.unique():
        user_df = alert[alert.user_id==user_id]
        event_summary = set(user_df.event_summary)
        if 'no_activity' in event_summary:
            activities = event_summary.copy()
            activities.remove('no_activity')
            for activity in activities:
                noactivity_high = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('ci95_hi')].values[0]
                noactivity_low = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('ci95_lo')].values[0]
                noactivity_meansleep = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('mean')].values[0]

                activity_high = user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('ci95_hi')].values[0]
                activity_low = user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('ci95_lo')].values[0]
                activity_meansleep=user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('mean')].values[0]



                noactivitycount = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('count')].values[0]
                activitycount = user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('count')].values[0]
                deg = (noactivitycount + activitycount - 2) #deegrees of freedom

                stdactivity = user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('std')].values[0]
                stdnoactivity = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('std')].values[0]
                std_N1N2 = sqrt( ((activitycount - 1)*(stdactivity)**2 + (noactivitycount - 1)*(stdnoactivity)**2) / deg) #average standard deviations between groups.
                diff_mean = abs(activity_meansleep-noactivity_meansleep)


                temp2['user_id']=user_id
                temp2['life_aspect']='sleep'
                temp2['timestampadded']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
                temp2['expirydate']=(datetime.datetime.now() + timedelta(days=7) ).strftime('%Y-%m-%d')
                temp2['view']='No'

                MoE = t.ppf(0.95, deg) * std_N1N2 * sqrt(1/(activitycount) + 1/(noactivitycount)) # margin of error 

                upperci= np.round(diff_mean+MoE,2)
                lowerci= np.round(diff_mean-MoE,2)



                if (0<lowerci):

                    msg = ''
                    if activity_meansleep > noactivity_meansleep:

                        msg = f"""{dct_activity[current_activity][activity][0]} to {dct_activity[current_activity][activity][1]} mins of {current_activity} increased your sleep by {int((activity_meansleep - noactivity_meansleep)*60)} mins"""

                    else:

                        msg = f"""{dct_activity[current_activity][activity][0]} to {dct_activity[current_activity][activity][1]} mins of {current_activity} reduced your sleep by {abs(int((activity_meansleep - noactivity_meansleep)*60))} mins"""

                    if "inf" in msg:
                        msg = "More than " + msg.replace("to inf", "")

                    temp2['insighttext'] = msg
                    
                    temp2['impact']=np.where("increased" in msg,'positive','negative')
                    
                    



                    display(temp2)
                    ds.append(copy.deepcopy(temp2))


                else:
                    pass
                
            return ds

df_final=[]
for insight_activity in dct_activity.keys():
    
    
    df_final.append(copy.deepcopy(insights_generate(insight_activity)))
    
insights=pd.DataFrame(df_final).stack().apply(pd.Series)

try:
    
    insights['impact']=insights['impact'].astype(str)
    insights=insights[['user_id','life_aspect','impact','insighttext','timestampadded','expirydate','view']].copy()
    insights=insights[insights.insighttext.notnull()]

except KeyError:
    pass




insights.to_sql(name='insights', con=engine, index=False,if_exists='append')


    
