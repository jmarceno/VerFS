import os
import traceback
from typing import Tuple
import trio
from utils import load_configuration
import traceback
import time

confs = load_configuration()

async def LogEvent(event:Tuple):
    try:
        with open(confs['log_file'], 'at') as f:
            print(str(time.time()) + ":" + str(event[0]) + "->" + str(event[1]), file=f)
    except:
        print(traceback.format_exc())
        pass
