# -*- coding: utf-8 -*-
"""
Created on Fri Oct  7 19:34:18 2022

@author: nbadam
"""

import numpy as np
import pandas as pd
from scatterplot_sleep_eventstream import *
from activity_sleep_scatterplot import *


def generate_scatterplotdata(streamtype,comparing_activity):
    if streamtype=='eventstream':
        df=eventstream_scatterplot(comparing_activity) #Enter event name like biking, running etc
        
    elif streamtype=='datastream':
        df=datastream_scatterplot(comparing_activity) #Enter table name like step_count etc

    return df

df_final=generate_scatterplotdata('datastream','step_count')
    


