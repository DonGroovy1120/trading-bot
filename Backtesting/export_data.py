import math
import json
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import time
import sqlalchemy
import sys
import os
_db_link = "mysql://root:"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_DATABASE']
engine = sqlalchemy.create_engine(_db_link)
connection = engine.connect()

if (len(sys.argv) == 0):
    print("Please enter coin name as parameter")
    sys.exit()
res = connection.execute(text("select coin_id from coinlist  where coin_name = :name"), name=sys.argv[1]).fetchall() 
if (len(res)==0):
    print("Coin not found")
    sys.exit()
    
_id = res[0]['coin_id']    
    
_sql = "Select prc_open_time, prc_open, prc_high, prc_low, prc_close from prices where prc_coin_id = :id"
res = connection.execute(text(_sql),id = _id)
df = pd.DataFrame(res.fetchall(), columns=['time','open', 'high', 'low', 'close' ]).set_index('time')
connection.close()
df.to_pickle('data/'+sys.argv[1]+'.pickle')


    