import math
import json
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import time
import sqlalchemy
import sys
import os
from sqlalchemy import create_engine

_db_link = "mysql://root:"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_DATABASE']
db_engine = create_engine(_db_link)
conneciton = db_engine.connect()

tab = []
_data = conneciton.execute("SELECT bkt_id, bkt_summary from backtests").fetchall()

for r in _data:
    print(r['bkt_id'])
    ## Trades
    summary = r['bkt_summary']
    if summary != None:
        lines = summary.split("\n")
        
        for l in lines:
            z = l.split("# Trades")
            if len(z) == 2:
                print(z[1])
                conneciton.execute(text("update backtests set bkt_win_amount = :amount where bkt_id = :id"), amount = int(z[1]), id = r['bkt_id'])
                
        
        
conneciton.close()
