# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 19:34:18 2022

@author: nbadam
"""

import numpy as np
import pandas as pd
from eventstream_sleep_scatterplot import *
from activity_sleep_scatterplot import *



def generate_scatterplotdata(temporal_time_hrs,comparing_activity, effect_activity,comparing_streamtype,effect_streamtype):
    if comparing_streamtype=='eventstream' and effect_streamtype=='eventstream':
        df=eventstream_scatterplot(comparing_activity,temporal_time_hrs,effect_activity) #Enter event name like biking, running etc
        
    elif comparing_streamtype=='datastream' and effect_streamtype=='eventstream':
        df=datastream_scatterplot(comparing_activity,temporal_time_hrs,effect_activity) #Enter table name like step_count etc

    return df


df_final=generate_scatterplotdata(10,'step_count','Sleep','datastream','eventstream')
    


