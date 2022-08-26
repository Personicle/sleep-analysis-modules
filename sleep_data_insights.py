# -*- coding: utf-8 -*-
"""
Created on Tue Aug 16 20:31:10 2022

@author: nbadam
"""

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

query_events=query_events='select * from  personal_events '
events_df= sqlio.read_sql_query(query_events,engine)

data=events_df.sort_values(by=['start_time']).copy()

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


data=data[(data.event_name!='')&(data.event_name.notnull())].copy()

events_df=data.copy()

del data

events_df['duration']=(events_df['end_time'] - events_df['start_time']) / pd.Timedelta(hours=1)

def timestamp_split(df):
    #df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = df['end_time'].dt.date
    df['month'] = df['end_time'].dt.month
    #df['hour'] = df['end_time'].dt.hour
    df['day_week'] = df['end_time'].dt.dayofweek
    df['day_name']=df['end_time'].dt.strftime("%A")
    return df


events_df=timestamp_split(events_df)
events_df.date=events_df.date.astype(str)

sleep_events=events_df[(events_df.event_name=='Sleep')&(events_df.duration!=0)&(events_df.duration<=15)].copy()

stats_sleep=[]
stats_sleep = sleep_events.groupby(['user_id','day_name','event_name'])['duration'].agg(['mean', 'count', 'std'])

ci95_hi = []
ci95_lo = []

for i in stats_sleep.index:
    m, c, s = stats_sleep.loc[i]
    ci95_hi.append(m + 1.96*s/math.sqrt(c))
    ci95_lo.append(m - 1.96*s/math.sqrt(c))

stats_sleep['ci95_hi'] = ci95_hi
stats_sleep['ci95_lo'] = ci95_lo

stats_sleep=stats_sleep[stats_sleep['count']>1].reset_index()

stats_sleep['max_mean']=stats_sleep.groupby(['user_id','event_name'])['mean'].transform(max)
stats_sleep['min_mean']=stats_sleep.groupby(['user_id','event_name'])['mean'].transform(min)

stats_sleep['summary']=np.where(stats_sleep['max_mean']==stats_sleep['mean'],'best_day',np.where(stats_sleep['min_mean']==stats_sleep['mean'],'worst_day','pass'))

alert=stats_sleep[stats_sleep.summary!='pass'].reset_index()

temp2={}
ds = []

for i in alert.user_id.unique():
    temp1=[]
    temp1=alert[alert.user_id==i]
    
    print(i)
    
    try:
    
        worstdayhi=temp1[temp1.summary=='worst_day'].iloc[:,temp1.columns.get_loc('ci95_hi')].values[0]
        worstdaylow=temp1[temp1.summary=='worst_day'].iloc[:,temp1.columns.get_loc('ci95_lo')].values[0]

        bestdayhi=temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('ci95_hi')].values[0]
        bestdaylow=temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('ci95_lo')].values[0]


        bestdaymeansleep=temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('mean')].values[0]
        worstdaymeansleep=temp1[temp1.summary=='worst_day'].iloc[:,temp1.columns.get_loc('mean')].values[0]

        bestday=temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('day_name')].values[0]
        worstday=temp1[temp1.summary=='worst_day'].iloc[:,temp1.columns.get_loc('day_name')].values[0]

        difference=round((bestdaymeansleep-worstdaymeansleep)*60)
        lifeaspect=temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('event_name')].values[0]
        
        
       

        
        
        
        
    
    except IndexError:
        continue  ##Running into error if neither of best_day or worst_day exists due to user being relatively new
         ##Found the issue for fw9529@wayne.edu	##Handled it

    
    
    if not((worstdayhi>=bestdaylow)&(worstdayhi<=bestdayhi)):
        
        
        temp2['user_id']=i
        temp2['life_aspect']=lifeaspect
        #temp2['impact']=
        temp2['timestampadded']=strftime("%Y-%m-%d %H:%M:%S", gmtime())
        temp2['expirydate']=(datetime.datetime.now() + timedelta(days=7) ).strftime('%Y-%m-%d')
        temp2['view']='No'
        
        if (bestdaymeansleep>8.0):
    
            temp2['impact']='positive'
            temp2['insighttext']='You got {} minutes better {} on {} than {} '.format(difference,lifeaspect,bestday,worstday)
            
            
        else:
                                                                   temp2['impact']='negative'
                                                                   temp2['insighttext']='You got {} minutes lesser {} on {} than {} '.format(difference,lifeaspect,worstday,bestday)
                                                
            
                
                  
        ds.append(copy.deepcopy(temp2))
      
        
    else:
        
        
        
        bestdaycount = temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('count')].values[0]
        worstdaycount = temp1[temp1.summary=='worst_day'].iloc[:,temp1.columns.get_loc('count')].values[0]
        deg = (bestdaycount + worstdaycount - 2) #deegrees of freedom
        stdbest = temp1[temp1.summary=='best_day'].iloc[:,temp1.columns.get_loc('std')].values[0]
        stdworst = temp1[temp1.summary=='worst_day'].iloc[:,temp1.columns.get_loc('std')].values[0]
        std_N1N2 = sqrt( ((bestdaycount - 1)*(stdbest)**2 + (worstdaycount - 1)*(stdworst)**2) / deg) #average standard deviations between groups.
        diff_mean = bestdaymeansleep-worstdaymeansleep
        
        MoE = t.ppf(0.975, deg) * std_N1N2 * sqrt(1/(bestdaycount) + 1/(worstdaycount)) # margin of error 
        
        upperci= np.round(diff_mean+MoE,2)
        lowerci= np.round(diff_mean-MoE,2)
            
        if 0>=lowerci and 0<=upperci:
            
            if (bestdaymeansleep>8.0):
                
                temp2['impact']='positive'
                temp2['insighttext']='You got {} minutes better {} on {} than {} '.format(difference,lifeaspect,bestday,worstday)

            
            else:
                temp2['impact']='negative'
                temp2['insighttext']='You got {} minutes lesser {} on {} than {} '.format(difference,lifeaspect,worstday,bestday)

                
                                                                   #temp2['impact']='negative'
                                                
            
                
          
        
        ds.append(copy.deepcopy(temp2))
      
    

        



insights=pd.DataFrame(ds)
insights=insights[['user_id','life_aspect','impact','insighttext','timestampadded','expirydate','view']].copy()

insights.to_sql(name='insights', con=engine, if_exists='replace', index=False)