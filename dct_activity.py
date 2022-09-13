# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 10:33:59 2022

@author: nbadam
"""

#Activity dictionaries that needs to be updated over time

import numpy as np

dct_activity={
        
        'running' : {'no_activity': [0,0], 'short_run': [1, 15], 'medium_run' : [16, 30], 'long_run':[31, np.Inf]},

'biking': {'no_activity':[0,0], 'short_bike': [1,30] ,'medium_bike': [31,60],'long_bike': [61,np.Inf]},

'mindfulness': {'no_activity':[0,0], 'short_mindfulness': [1,5] ,'medium_mindfulness': [6,10],'long_mindfulness': [11,np.Inf]},
 
'strengthtraining': {'no_activity':[0,0], 'short_strengthtraining': [11,20] ,'medium_strengthtraining': [21,50],'long_strengthtraining': [51,np.Inf]}

    
    }