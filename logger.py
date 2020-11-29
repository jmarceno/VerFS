import os
import traceback
from typing import Tuple
import trio
from utils import load_configuration
import traceback
import datetime

confs = load_configuration()

def LogEvent(event:Tuple):
    try:
        with open(confs['log_file'], 'at') as f:
            print(str(datetime.datetime.now()) + ":" + str(event[0]) + "->" + str(event[1]), file=f)
    except:
        print(traceback.format_exc())
        pass
