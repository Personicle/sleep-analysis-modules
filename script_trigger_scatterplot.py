# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 19:34:18 2022

@author: nbadam
"""

import numpy as np
import pandas as pd
#from eventstream_sleep_scatterplot import * #e2escript same as e2e_scatterplot 
#from activity_sleep_scatterplot import * #d2escript #same as d2e_scatterplot
#delete the eventstream_sleep_scatterplot and activity_sleep_scatterplot from repository 
#as I renamed them for correct representation
from d2d_scatterplot import *
from e2e_scatterplot import *
from e2d_scatterplot import *
from d2e_scatterplot import *



def generate_scatterplotdata(temporal_time_hrs,comparing_activity, effect_activity,comparing_streamtype,effect_streamtype):
    if comparing_streamtype=='eventstream' and effect_streamtype=='eventstream':
        df=e2e_scatterplot(comparing_activity,temporal_time_hrs,effect_activity) #Enter event name like biking, running etc
        
    elif comparing_streamtype=='datastream' and effect_streamtype=='eventstream':
        df=d2e_scatterplot(comparing_activity,temporal_time_hrs,effect_activity) #Enter table name like step_count etc

    elif comparing_streamtype=='eventstream' and effect_streamtype=='datastream':
        df=e2d_scatterplot(comparing_activity,temporal_time_hrs,effect_activity)
        
    elif comparing_streamtype=='datastream' and effect_streamtype=='datastream':
        df=d2d_scatterplot(comparing_activity,temporal_time_hrs,effect_activity)

    
    return df


#df_final=generate_scatterplotdata(10,'step_count','Sleep','datastream','eventstream')
 
df_final=generate_scatterplotdata(24,'interval_step_count','heart_rate','datastream','datastream')
   


