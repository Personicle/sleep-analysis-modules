# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 10:33:59 2022

@author: nbadam
"""

#Activity dictionaries that needs to be updated over time

import numpy as np
import pandas as pd
import json
import math
from time import gmtime, strftime
from datetime import datetime
from datetime import timedelta, date
from math import sqrt
from scipy.stats import t
#from scipy.stats import st

from base_schema import *
from db_connection import *
from sqlalchemy import select
import pandas.io.sql as sqlio



def datastream(tblname):
    datastream='select * from '+tblname 
    datastream= sqlio.read_sql_query(datastream,engine)
    
    return datastream



def timestamp_modify(datastream):
    if 'timestamp' in datastream.columns:
        
        if any("perminute" in s for s in datastream.unit.unique()):        
            l=list(filter(lambda x: "perminute" in x, datastream.unit.unique()))
            print(l)
            datastream=datastream[datastream.unit==l[0]]
 
            datastream['start_time']=datastream['timestamp']-pd.Timedelta('1min')
            datastream.rename(columns={'timestamp':'end_time','individual_id':'user_id'},inplace=True)
            
            
        else:
            datastream['start_time']=datastream['timestamp']
            datastream.rename(columns={'timestamp':'end_time','individual_id':'user_id'},inplace=True)

            
            
        datastream=datastream[datastream.unit.notnull()]
            
        return datastream[['user_id','start_time','end_time','source', 'value', 'unit', 'confidence']]
    
    else:
        datastream.rename(columns={'individual_id':'user_id'},inplace=True)
        
    datastream=datastream[datastream.unit.notnull()]
    return datastream
        


def events_overlap(eventstream):
    
    eventstream=eventstream[(eventstream.event_name!='')&(eventstream.event_name.notnull())]

    data=eventstream.sort_values(by=['start_time']).copy()

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

    return data



def getCategory(num, dct):
    
    for key in dct.keys():
        if dct[key][0] <= num <= dct[key][1]:
            return key


def insights_generate(insight_activity,pivot_sleep,ci):
    current_activity=insight_activity #change here
    
    alpha=(1-(ci)*0.01)
    zscore=round(stats.norm.ppf((1-(alpha)/2)),2)
    stats_sleep=[]
    stats_sleep = pivot_sleep.groupby(['user_id',current_activity])['sleep_duration'].agg(['mean', 'count', 'std'])
    #display(stats_sleep)

    # ci95_hi = []
    # ci95_lo = []
    
    ci_hi=[]
    ci_lo=[]

    for i in stats_sleep.index:
        m, c, s = stats_sleep.loc[i]
        # ci95_hi.append(m + 1.96*s/math.sqrt(c)) #In a variable define a multiplier
        # ci95_lo.append(m - 1.96*s/math.sqrt(c))
        
        ci_hi.append(m + zscore*s/math.sqrt(c)) #In a variable define a multiplier
        ci_lo.append(m - zscore*s/math.sqrt(c))

        
    # stats_sleep['ci95_hi'] = ci95_hi
    # stats_sleep['ci95_lo'] = ci95_lo
    
    stats_sleep['ci_hi'] = ci_hi
    stats_sleep['ci_lo'] = ci_lo
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
                noactivity_high = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('ci_hi')].values[0]
                noactivity_low = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('ci_lo')].values[0]
                noactivity_meansleep = user_df[user_df.event_summary=='no_activity'].iloc[:,user_df.columns.get_loc('mean')].values[0]

                activity_high = user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('ci_hi')].values[0]
                activity_low = user_df[user_df.event_summary==activity].iloc[:,user_df.columns.get_loc('ci_lo')].values[0]
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
                    
                    

                    ds.append(copy.deepcopy(temp2))


                else:
                    pass
                
            return ds
        
        
        



