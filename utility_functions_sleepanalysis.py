# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 19:32:11 2022

@author: nbadam
"""

import numpy as np
import pandas as pd


dct_activity={
        
        'running' : {'no_activity': [0,0], 'short_run': [1, 15], 'medium_run' : [16, 30], 'long_run':[31, np.Inf]},

'biking': {'no_activity':[0,0], 'short_bike': [1,30] ,'medium_bike': [31,60],'long_bike': [61,np.Inf]},

'mindfulness': {'no_activity':[0,0], 'short_mindfulness': [1,5] ,'medium_mindfulness': [6,10],'long_mindfulness': [11,np.Inf]},
 
'strengthtraining': {'no_activity':[0,0], 'short_strengthtraining': [11,20] ,'medium_strengthtraining': [21,50],'long_strengthtraining': [51,np.Inf]},

    'Walking': {'no_activity':[0,0], 'short_walk': [11,20] ,'medium_walk': [21,50],'long_walk': [51,np.Inf]},
    
    #'steps':{'no_activity':[0,0], 'shortduration_steps': [1,600] ,'mediumduration_steps': [601,822],'longduration_steps': [823,np.Inf]}
    'stepscount':{'no_activity':[0,0], 'low_steps_perday': [1,4300] ,'medium_steps_perday': [4301,10000],'high_steps_perday': [10301,np.Inf] }
    
    
}



#Dictionary to map units and activity
eventname_unit={
    
    'stepsperminute':'steps',
    'kilo_calories':'calories',
    'meters':'distance'
    }

#Include activity type that needs to be summed up
activity_cumulative=['steps']

#Dictionary to map analysisid by activity:
    
activity_analysisid={
     'biking':1,
     'steps':2,
    'calories':3,
    'distance':4
     
     }   

