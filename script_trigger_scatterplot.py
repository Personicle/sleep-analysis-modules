# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 19:34:18 2022

@author: nbadam
"""

import numpy as np
import pandas as pd
from eventstream_sleep_scatterplot import *
from activity_sleep_scatterplot import *


def generate_scatterplotdata(streamtype,comparing_activity,temporal_time_hrs):
    if streamtype=='eventstream':
        df=eventstream_scatterplot(comparing_activity,temporal_time_hrs) #Enter event name like biking, running etc
        
    elif streamtype=='datastream':
        df=datastream_scatterplot(comparing_activity,temporal_time_hrs) #Enter table name like step_count etc

    return df

df_final=generate_scatterplotdata('datastream','step_count',10)
    


