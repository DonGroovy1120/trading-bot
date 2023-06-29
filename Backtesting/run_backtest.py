import math
import json
from sqlalchemy.sql import text
from backtesting import Backtest, Strategy
import pandas as pd
import numpy as np
import time
import sqlalchemy
import sys
import os
import asyncio
import time
from multiprocessing import cpu_count
import concurrent.futures
import random
def to_datetime(date):
    try:
    
        timestamp = ((date - np.datetime64('1970-01-01T00:00:00'))
                    / np.timedelta64(1, 's'))
        return timestamp
    except:
        return 0
NUM_CORES = 1
_db_link = "mysql://root:"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_DATABASE']
engine = sqlalchemy.create_engine(_db_link)
connection_global = engine.connect()
#all_global_trades = []
all_tests = 0

_last_saved_progress =0 
_test_10_zero_in_row = 0


def save_progress_multi(bkt_id, connection):
    
    global all_tests    
    global _last_saved_progress
    res= connection.execute(text("select count(*) il from backtest_tmp where bkt_id = :id and type = 'T'"), id = bkt_id).first()
    tests_done = res['il']
    
    progress = 0
    if all_tests>0:
        progress = math.floor(tests_done/all_tests*100)
    print(tests_done, all_tests)
    if _last_saved_progress != progress:
        
        connection.execute(text("UPDATE backtests set bkt_progress = :progress where bkt_id = :id"), progress = progress, id = bkt_id)        
        _last_saved_progress = progress    
        
    
    
    
class DivergenceStrategy(Strategy):
    trade_size = 100
    min_score = 10
    atrp_tf = 0
    sl_atrp = 100
    tp_atrp = 100
    sl_percentage = 1
    tp_percentage = 1
    direction =  0
    with_trend = 0
    kill_trade = 10
    bkt_id = 0
    score_type = 'S'
    cooldown_time = 0
    btc_push_threshold = -0.05
    part_of_multi = 0
    coin_id = 0
    records_amouont = 0
    connection = None
    time1 = '00:00'
    time2 = '00:00'
    time3 = '00:00'
    days = '[]'
    def init(self):        
        self.counter = 0
        self._last_status_checked = 0
        self._last_trade_index = 0
        self.trade_flag = False
        self._all_trades = []
        self._last_saved_progress = 0
        self.trading_days = []
        for d in json.loads(self.days):
            self.trading_days.append(int(d))
        print(self.trading_days)
    def save_progress(self,value, bkt_id, all_records, connection):
                
            progress = 0
            if all_records>0:
                progress = math.floor(value/all_records*100)
            
            if self._last_saved_progress != progress:
                
                connection.execute(text("UPDATE backtests set bkt_progress = :progress where bkt_id = :id"), progress = progress, id = bkt_id)        
                self._last_saved_progress = progress
        
    def add_to_global_data(self,value, size, pl, pl_pct,entry_time, exit_time,entry_price, exit_price,_sl,_tp):
            
            rec = {}
            rec['val'] = value
            rec['sz'] = size
            rec['pl'] = pl
            rec['pl_pct'] = pl_pct
            rec['en_tm'] = to_datetime(entry_time)
            rec['ex_tm'] = to_datetime(exit_time)
            rec['en_pr'] = entry_price
            rec['ex_pr'] = exit_price
            rec['tp'] = _tp
            rec['sl'] = _sl
            self._all_trades.append(rec)

    def trend(self):        
        _sm_value = self.data['sma'][-1]
        if _sm_value == 0:
            return 0
        if len(self.data['close_btc'])>2:
            if (self.data['close_btc'][-1]<_sm_value) and (self.data['close_btc'][-2]<_sm_value) and (self.data['open_btc'][-1]<_sm_value) and (self.data['open_btc'][-2]<_sm_value):
                    return -1
            if (self.data['close_btc'][-1]>_sm_value) and (self.data['close_btc'][-2]>_sm_value) and (self.data['open_btc'][-1]>_sm_value) and (self.data['open_btc'][-2]>_sm_value):
                    return 1
        return 0
    def trend_confirmation(self):
        if self.with_trend == 1:
            if (self.data['sma_confirmation'][-1]<self.data['Close'][-1]):
                return 1
            if (self.data['sma_confirmation'][-1]>self.data['Close'][-1]):
                return -1
        if self.with_trend == 2:
            #Check trend with EMA 8,21,34
            
            if (self.data['EMA_8'][-1] > self.data['EMA_21'][-1]):
                if (self.data['EMA_21'][-1] > self.data['EMA_34'][-1]):
                    return 1
        
            
            if (self.data['EMA_8'][-1] < self.data['EMA_21'][-1]):
                if (self.data['EMA_21'][-1] < self.data['EMA_34'][-1]):
                    return -1
        
                    
        return 0
    def check_convergance(self, _dir):
        _length = len(self.data['score'])  - 3
        while (_length>0):
            if _dir == 'S':
                if self.data['score'][_length] <= -1*self.min_score:
                    if self.data['score'][_length-1] > -1*self.min_score:
                        return ''
                if (self.data['score'][_length] >= 0):
                    return _dir
            if _dir == 'B':
                if self.data['score'][_length] >= self.min_score:
                    if self.data['score'][_length-1] < self.min_score:
                        return ''
                if (self.data['score'][_length] <= 0):
                    return _dir        
            _length -= 1
        return ''
                
    def save_trades(self):           
        
        for t in self.closed_trades:        
            self.add_to_global_data(t.value, t.size, t.pl, t.pl_pct,t.entry_time, t.exit_time,t.entry_price, t.exit_price, t.sl, t.tp)
        for t in self.trades:
            self.add_to_global_data(t.value, t.size, t.pl, t.pl_pct,t.entry_time, t.exit_time,t.entry_price, t.exit_price, t.sl,t.tp)
            
    def next(self):
        
        self.counter += 1
        if self.bkt_id>0:
                if self._last_status_checked != math.floor(time.time()/2):
                    
                    self._last_status_checked = math.floor(time.time()/2)
                    conn2 = engine.connect()
                    res = conn2.execute(text("select bkt_status from backtests where bkt_id = :id"), id = self.bkt_id).one()
                    if (len(res)>0):                
                        if (res['bkt_status'] == 'B'):
                            
                            self.connection.close()
                            conn2.close()
                            sys.exit()
                    conn2.close()
        if (self.part_of_multi == 0) and (self.bkt_id>0):
            
            self.save_progress(self.counter, self.bkt_id, self.records_amouont, self.connection)   
        
        
        if self.trade_flag:
            if self.position.size == 0:
                self._last_trade_index = self.counter
                self.trade_flag = False
        cooled = True        
        if self.cooldown_time>0:
            
            if self._last_trade_index>0:
                cooled = (self.counter-self._last_trade_index)>self.cooldown_time
#        if (abs(self.data['score'][-1])>=self.min_score) or (abs(self.data['percentage_for_long'][-1])>=self.min_score) or (abs(self.data['percentage_for_short'][-1])>=self.min_score):     
        
        if (self.position.size == 0) and cooled:                                
            _dir = ''
            
            if self.score_type == 'S':
                if (abs(self.data['score'][-1])>=self.min_score):
                    _change = self.data['change'][-1]                    
                    if _change<0:
                        _dir = 'S'
                    else:
                        _dir = 'B'
            if self.score_type == 'Z':
                if (abs(self.data['score'][-1])>=self.min_score):
                    if self.data['score']<0:
                            _dir = 'S'
                    else:
                            _dir = 'B'
            if self.score_type == 'C':
                if (len(self.data['score'])>1):
                        if self.data['score'][-1]>=self.min_score:
                            if (self.data['score'][-2]<self.min_score):
                                _dir = 'B'
                        if self.data['score'][-1]<=-1*self.min_score:
                            if (self.data['score'][-2]>-1*self.min_score):
                                _dir = 'S'
                        if (_dir != ''):
                            _dir = self.check_convergance(_dir)
                        
            if self.score_type == 'P':
                if (self.data['percentage_for_long'][-1])>=self.min_score:                        
                    if self.trend() != 1:                            
                        _dir = 'B'
                        
                if (self.data['percentage_for_short'][-1])>=self.min_score:
                    if self.trend() != -1:
                        _dir = 'S'
                        
            if self.score_type == 'B':
                if (self.data['percentage_for_short'][-1])>=self.min_score:                        
                    if self.btc_push_threshold>=0:
                        if (self.data['percentage_btc_for_short'] <= self.btc_push_threshold) and (self.data['percentage_btc_for_long']>0):
                            _dir = 'S'
                    if self.btc_push_threshold<0:
                        if (self.data['percentage_btc_for_long']>=abs(self.btc_push_threshold)):
                            _dir = 'S'

                if (self.data['percentage_for_long'][-1])>=self.min_score:                        
                    if self.btc_push_threshold>=0:                            
                        if (self.data['percentage_btc_for_long'] <= self.btc_push_threshold) and (self.data['percentage_btc_for_short']>0):
                            _dir = 'B'
                    if self.btc_push_threshold<0:                            
                        if (self.data['percentage_btc_for_short']>=abs(self.btc_push_threshold)):
                            _dir = 'B'

            if self.score_type == 'V':
                
                if (abs(self.data['close_zscore'])>=self.min_score):
                    
                    if (self.data['close_zscore']>0):
                        if self.btc_push_threshold>=self.data['close_btc_zscore']:
                            _dir = 'B'
                            
                        
                    if (self.data['close_zscore']<0):
                        if (self.btc_push_threshold*-1)<=self.data['close_btc_zscore']:
                            _dir = 'S'

                    


                    
                


            if self.with_trend > 0:                
                if _dir == 'B' and self.trend_confirmation() != 1:
                    _dir = ''
                if _dir == 'S' and self.trend_confirmation() != -1:                        
                    _dir = ''
     
            if self.score_type == 'I':
                    
                if self.direction == -1:
                    _dir = 'S'
                if self.direction == 1:
                    _dir = 'B'
                if self.direction == 0:
                    if random.randint(0,10)>=4:
                        _dir = 'B'
                    else:
                        _dir = 'S'
                        
            if self.score_type == 'T':
                _times = [self.time1,self.time2,self.time3]
                _curr = str(self.data.index[-1]).split(' ')
                if len(_curr) == 2:
                    _tmb = _curr[1].split(':')
                    if len(_tmb)>1:
                        _run_trade = False
                        for _t in _times:
                            _tm = _t.split(':')
                            if (len(_tm) == 2):
                                if (int(_tm[0]) == int(_tmb[0])) and (int(_tm[1]) == int(_tmb[1])):
                                    _run_trade = True
                                    break
                                                                    
                        if _run_trade:
                            if self.direction == -1:
                                _dir = 'S'
                            if self.direction == 1:
                                _dir = 'B'
                            if self.direction == 0:
                                if random.randint(0,10)>=4:
                                    _dir = 'B'
                                else:
                                    _dir = 'S'
                        



            if self.score_type == 'D':
                _times = [self.time1,self.time2,self.time3]
                _curr = str(self.data.index[-1]).split(' ')
                
                
                if len(_curr) == 2:
                    _tmb = _curr[1].split(':')
                    if len(_tmb)>1:
                        _run_trade = False
                        for _t in _times:                            
                            _tm = _t.split(':')
                            if (len(_tm) == 2):
                                
                                if (int(_tm[0]) == int(_tmb[0])) and (int(_tm[1]) == int(_tmb[1])):
                                        _run_trade = True
                                        break
                        if _run_trade:
                            _weekday = self.data.index[-1].weekday()+1
                            if _weekday not in self.trading_days:
                                _run_trade = False
                            
                        if _run_trade:
                            if self.direction == -1:
                                _dir = 'S'
                            if self.direction == 1:
                                _dir = 'B'
                            if self.direction == 0:
                                if random.randint(0,10)>=4:
                                    _dir = 'B'
                                else:
                                    _dir = 'S'


                    
                        
                        
            if (_dir == 'B') or (_dir == 'S'):
                    
                    if (self.direction == 0) or ((self.direction == -1) and (_dir == 'S')) or ((self.direction == 1) and (_dir == 'B')):
                            
                            _sl_perc = self.sl_percentage
                            _tp_perc = self.tp_percentage
                            if self.atrp_tf != 0:
                                _sl_perc = self.data['atrp'][-1]*(self.sl_atrp/100)
                                _tp_perc = self.data['atrp'][-1]*(self.tp_atrp/100)
                            
                            _sl_distance = self.data['Close'][-1]*(_sl_perc/100)
                            _tp_distance = self.data['Close'][-1]*(_tp_perc/100)
                            
                            try:    
                                if _dir == 'B':
                                    print("Openinig new buy position at: "+str(self.data.index[-1]))                    
                                    _sl = self.data['Close'][-1]-_sl_distance
                                    _tp = self.data['Close'][-1]+_tp_distance
                                    
                                    self.buy(size = self.trade_size, sl = _sl, tp= _tp)
                                    
                                    
                                if _dir == 'S':
                                    print("Openinig new sell position at: "+str(self.data.index[-1]))
                                    _sl = self.data['Close'][-1]+_sl_distance
                                    _tp = self.data['Close'][-1]-_tp_distance
                                    
                                    self.sell(size = self.trade_size, sl = _sl, tp= _tp)
                                
                                self.trade_flag = True
                            except Exception as e:
                                print("Errror "+str(e))
                                if self.bkt_id>0:
                                    self.connection.execute(text("update backtests set bkt_status = 'E', bkt_error='"+str(e).replace("'","\'")+"' where bkt_id = :id"), id = self.bkt_id)
                                    self.connection.close()
                                    sys.exit()
                                
        
        if self.kill_trade>0:    
            for trade in self.trades:                                    
                diff = (self.data.index[-1]-trade.entry_time).seconds/60                
                if diff>=self.kill_trade:
                    print("Kill trade")
                    trade.close()
                    break
        
        if (self.counter>=self.records_amouont-1) and (self.bkt_id>0):
            print("Save information")
            self.save_trades()
            print("Save JSON")
            st = ''
            if self.part_of_multi == 0: 
                st = json.dumps(self._all_trades)                                
            else:
                st = json.dumps({'id':self.coin_id, 'trades':self._all_trades})
            print("Saving backtest tmp id: ",self.coin_id)
            self.connection.execute(text("INSERT INTO backtest_tmp (bkt_id, result, type) VALUES (:_id , :_text, 'T')"), _id = self.bkt_id, _text = st )
        
                
   
                
        
    
def _data_period(_coin, _from,_to,connection):
    print("Getting data from DB")
    _time = time.time()
    
    res = connection.execute("select coin_id from coinlist  where coin_name = 'BTCUSDT'").fetchall()    
    print("Get coins ", str(time.time()-_time))
    if (len(res)==0):
        return None    
    
    _id_btc = res[0]['coin_id']    
    _time = time.time()
    print("Starting downloading data ",_from,"  ",_to,"  ",_coin)
    _sql = "Select  prc_open_time, prc_open, prc_high, prc_low, prc_close from prices where prc_coin_id = :id and (prc_open_time BETWEEN :_from AND :_to)"
    res = connection.execute(text(_sql), _from = _from, _to = _to, id = _coin)
    print("Get prices from DB ", str(time.time()-_time))
    
    _time = time.time()
    df = pd.DataFrame(res.fetchall(), columns=['time','open', 'high', 'low', 'close' ]).set_index('time')
    print("Fetched ", str(time.time()-_time))
    
    _time = time.time()    
    res_btc = connection.execute(text(_sql), _from = _from, _to = _to, id = _id_btc)    
    print("Get BTC from DB ", str(time.time()-_time))
    
    _time = time.time()    
    
    df_btc = pd.DataFrame(res_btc.fetchall(), columns=['time','open_btc', 'high_btc', 'low_btc', 'close_btc' ]).set_index('time')
    print("FETCH BTC ", str(time.time() - _time))
    _time = time.time()    
    df_new = df.join(df_btc).fillna(method='ffill')
    print("JOINED  ", str(time.time()-_time))
    df_new.index = pd.to_datetime(df.index, unit='s')
    return df_new


def calculate_score(params):    
    if (len(params)==0):        
        return 0
    if math.isnan(params['change_btc']):
        return 0
    if math.isnan(params['change']):
        return 0

    btc = round(params['change_btc'])
    alt = round(params['change'])
    
    if abs(btc)==0 and abs(alt)>=1:
        if abs(alt)>10:
            return 10
        else:
            return abs(alt)
    if abs(btc)>=1 and abs(alt)>=1:        
        if ( (btc<0) and (alt>0) ) or ((btc>0) and (alt<0)):            
             score = min(abs(btc),abs(alt))*10
             return min(score,100)             
    return 0
    

def _prepare_data_for_signals(period, tm_atr, df, score_type = 'S', atrp_period = 14, sma_period = 100, sma_confirmation = 100, multi = 0, _bkt_id=0):
    print("BKT ID: ", _bkt_id)
    if tm_atr>0:
            
            df_tm = df.groupby(pd.Grouper( freq=str(tm_atr)+"Min",closed='left',label='left')).agg({
                                                    "open":  "first",
                                                    "high":  "max",
                                                    "low":   "min",
                                                    "close": "last"                               
            }).dropna()
            
            high_low = df_tm['high'] - df_tm['low']
            high_close = np.abs(df_tm['high'] - df_tm['close'].shift())
            low_close = np.abs(df_tm['low'] - df_tm['close'].shift())
            ranges = pd.concat([high_low, high_close, low_close], axis=1)
            true_range = np.max(ranges, axis=1)

            df_tm['atr'] = true_range.rolling(atrp_period).sum()/atrp_period
            df_tm['atrp'] = ((df_tm['atr']/df_tm['close'])*100)    

            df_tm=df_tm[[ 'atr', 'atrp']]
            df = df.join(df_tm).fillna(method='ffill')
    else:
        df['atrp'] = 0
    
    df['open_shift'] = df['open'].shift(period)
    df['open_btc_shift'] = df['open_btc'].shift(period)
    
    df['score'] = 0
    df['change'] = ((df['close'] - df['open_shift'])/df['close'])*100
    df['change_btc'] = ((df['close_btc'] - df['open_btc_shift'])/df['close_btc'])*100
    if score_type == 'S':
        df['score'] = df.apply(calculate_score, axis=1)
    df['close_zscore'] = 0
    df['close_btc_zscore'] = 0
    if (score_type == 'Z') or (score_type == 'V') or (score_type == 'C'):
        df['close_mean']  = df['close'].rolling(window=period).mean()
        df['close_std']  = df['close'].rolling(window=period).std()
        df['close_zscore'] = (df['close']-df['close_mean'])/df['close_std']

        df['close_btc_mean']  = df['close_btc'].rolling(window=period).mean()
        df['close_btc_std']  = df['close_btc'].rolling(window=period).std()            
        df['close_btc_zscore'] = (df['close_btc']-df['close_btc_mean'])/df['close_btc_std']
        
        df['score'] =  df['close_zscore'] - df['close_btc_zscore']
        df.replace([np.inf, -np.inf], 0, inplace=True)
        
    df['sma'] = 0
    df['percentage_for_long'] = 0
    df['percentage_for_short'] = 0
    df['percentage_btc_for_long'] = 0
    df['percentage_btc_for_short'] = 0
    df['sma_confirmation'] = 0
    df['EMA_8'] = 0
    df['EMA_21'] = 0
    df['EMA_34'] = 0
    #add EMA trend confirmation
    
    if sma_confirmation>0:
        df['sma_confirmation'] = df['close'].rolling(window=sma_confirmation).mean()
        df['EMA_8'] = df['close'].ewm(span = 8, adjust = False).mean()
        df['EMA_21'] =  df['close'].ewm(span = 21, adjust = False).mean()
        df['EMA_34'] = df['close'].ewm(span = 34, adjust = False).mean()

    if (score_type == 'P') or (score_type == 'B'):
        if (sma_period > 0) and (score_type == 'P'):
                df['sma'] = df['close_btc'].rolling(window=sma_period).mean()
        df['min_pick']=df['low'].rolling(window=period).min()
        df['max_pick']=df['high'].rolling(window=period).max()
        df['percentage_for_long'] = (df['close'] - df['min_pick'])/df['close']*100
        df['percentage_for_short'] = (df['max_pick']-df['close'])/df['close']*100
        if score_type == 'B':
            #add push BTC
            df['min_pick_btc'] = df['low_btc'].rolling(window=period).min()
            df['max_pick_btc'] = df['high_btc'].rolling(window=period).min()
            df['percentage_btc_for_long'] = (df['close_btc'] - df['min_pick_btc'])/df['close_btc']*100
            df['percentage_btc_for_short'] = (df['max_pick_btc'] - df['close_btc'])/df['close_btc']*100
            
        
        
    df = df.dropna()
    if (multi == 0) and (_bkt_id>0):
            file_name = str(_bkt_id)
            if not os.path.exists('/app/pickles/'+file_name[0]):
                os.mkdir('/app/pickles/'+file_name[0])
            file_name = '/app/pickles/'+file_name[0]+'/'+file_name
            df.to_pickle(file_name)
    
    df = df[['open','high','low','close','atrp','score','change','sma','percentage_for_long','percentage_for_short','close_btc','open_btc','sma_confirmation', 'percentage_btc_for_long', 'percentage_btc_for_short', 'close_btc', 'close_btc_zscore','close_zscore', 'EMA_8','EMA_21', 'EMA_34']]
    df = df.rename(columns = {"open":"Open", "high":"High", "low":"Low", "close":"Close"})
    print("Data prepared")
    
    
    
    
    return df
async def run_strategy(_strategy,coin_id, part_of_multi,connection):
    
    global _test_10_zero_in_row    
    
    if (_test_10_zero_in_row>=10):
        if part_of_multi == 1:
            save_progress_multi( _strategy['bkt_id'], connection)

        return coin_id, None, ""
    
    res = connection.execute(text("select bkt_status from backtests where bkt_id = :id"), id = _strategy['bkt_id']).one()
    if (len(res)>0):                
        if (res['bkt_status'] == 'B'):
            print("test break")
            connection.close()            
            sys.exit()

    _bkt_id = _strategy['bkt_id']
    _data =  _data_period(coin_id,_strategy['bkt_time_start'],_strategy['bkt_stop_time'],connection)
    if part_of_multi == 0:
        if len(_data) == 0:
            print("no data for selected period")
            connection.execute(text("update backtests set bkt_status = 'E', bkt_error='Data do not exists for selected period' where bkt_id = :id"), id = _bkt_id)
            return None
    _multi = 0
    if part_of_multi == 1:
        _multi = 1
    
    _data =  _prepare_data_for_signals(_strategy['bkt_period'], _strategy['bkt_atrp_tf'], _data, _strategy['bkt_score_type'], 14, _strategy['bkt_sma_length'], _strategy['bkt_trend_sma_length'], _multi , _bkt_id)
    _records = len(_data)
    
    print("Start backetst got:", str(_records)," records.")
    bt = None
    _output = ''
    
    if (_records>0):            
            _test_10_zero_in_row = 0
            bt = Backtest(_data,DivergenceStrategy,
                            cash = _strategy['bkt_equity'],
                            commission = _strategy['bkt_commission']/100,                     
                            exclusive_orders = True
                            )
            _output = bt.run(trade_size = _strategy['bkt_trade_size'],
                            min_score = _strategy['bkt_min_score'],
                            atrp_tf = _strategy['bkt_atrp_tf'],
                            sl_atrp = _strategy['bkt_sl_atrp'],
                            tp_atrp = _strategy['bkt_tp_atrp'],
                            sl_percentage = _strategy['bkt_sl_percentage'],
                            tp_percentage = _strategy['bkt_tp_percentage'],
                            direction = _strategy['bkt_direction'],
                            with_trend = _strategy['bkt_with_trend'],
                            kill_trade = _strategy['bkt_kill_trade'],
                            bkt_id = _strategy['bkt_id'],
                            score_type = _strategy['bkt_score_type'],
                            cooldown_time = _strategy['bkt_cooldown_time'],
                            btc_push_threshold = _strategy['bkt_btc_threshold'],
                            part_of_multi = _multi,
                            coin_id = coin_id,
                            records_amouont = _records,
                            connection = connection,
                            time1 = _strategy['bkt_time1'],
                            time2 = _strategy['bkt_time2'],
                            time3 = _strategy['bkt_time3'],
                            days = _strategy['bkt_daytrading']                            
                            )
            if part_of_multi == 0:
                    _sql = "update backtests set bkt_status = 'Z' , bkt_summary = :output"
                    _sql += ", bkt_sharpe_ratio = :bkt_sharpe_ratio"
                    _sql += ", bkt_proft_factor = :bkt_profit_factor"
                    _sql += ", bkt_win_rate = :bkt_win_rate"
                    _sql += ", bkt_win_amount = :bkt_win_amount"

                    _sql += " where bkt_id = :bkt_id"
                    _bkt_win_rate = _output['Win Rate [%]']
                    _bkt_sharpe_ratio = _output['Sharpe Ratio']
                    _bkt_profit_factor = _output['Profit Factor']
                    _bkt_win_amount = _output['# Trades']
                    if math.isnan(_bkt_win_rate): 
                            _bkt_win_rate = 0
                    if math.isnan(_bkt_sharpe_ratio): 
                            _bkt_sharpe_ratio = 0
                    if math.isnan(_bkt_profit_factor): 
                            _bkt_profit_factor = 0
                    file_name = '/tmp/plot_bk_'+str(_bkt_id)+'.html'
                    connection.execute(text(_sql), output = _output, bkt_id = _bkt_id,bkt_win_rate = _bkt_win_rate, bkt_sharpe_ratio = _bkt_sharpe_ratio ,bkt_profit_factor = _bkt_profit_factor,  bkt_win_amount = _bkt_win_amount)

                    _webpage = 'No Data'
                    if (bt != None):
                        bt.plot(filename=file_name, open_browser= False)
                        f=open(file_name,"r")
                        _webpage = f.read()    
                        f.close()
                        os.remove(file_name)
                    connection.execute(text("update backtests set bkt_webpage = :webpage where bkt_id = :bkt_id"),webpage = _webpage, bkt_id = _bkt_id)
            if part_of_multi == 1:
                 st = json.dumps({'output':str(_output), 'id':coin_id})
                 connection.execute(text("INSERT INTO backtest_tmp (bkt_id, result, type) VALUES (:_id , :_text, 'O')"), _id = _bkt_id, _text = st )

            
                
    else:
        _test_10_zero_in_row += 1
        if part_of_multi == 0:
            connection.execute(text("update backtests set bkt_status = 'E', bkt_error='Empty dataset for backtesting' where bkt_id = :id"), id = _bkt_id)
    
    if part_of_multi == 1:
        
        save_progress_multi( _strategy['bkt_id'], connection)
    
    #return coin_id, bt, _output
async def run_single_await(res,coins,_connection,part_of_multi):
    
    loops = [run_strategy(res, id, part_of_multi,_connection) for id in coins]
    await asyncio.gather(*loops)   
def run_single_core(res, coins,part_of_multi):

    if (len(coins)==0):
        return
    global engine        
    _connection = engine.connect()
    asyncio.run(run_single_await(res,coins,_connection,part_of_multi))         
    _connection.close()
    
    
def main():
    global all_tests
    global _test_10_zero_in_row
    
    ########### tylko for test ########
#    connection.execute(text("update backtests set bkt_status = 'O'"))
    ###########
    _bkt_id = 0
    res = connection_global.execute(text("select min(bkt_id) from backtests where bkt_status = 'O'")).fetchall()
    for r in res:
        _bkt_id = r[0]
        
    if _bkt_id == 0 or _bkt_id == None:
        print("No strategy to process")
        return
        
    
    connection_global.execute(text("update backtests set bkt_status = 'P', bkt_progress = 0 where bkt_id = :id"), id = _bkt_id)
    connection_global.execute(text("delete from  backtest_tmp where bkt_id= :_id "), _id = _bkt_id )
    print("preparing data")
    _sql = "select bkt_id, bkt_coin, bkt_coins, bkt_time_start, bkt_stop_time, bkt_period, bkt_atrp_tf,  "
    _sql += " bkt_trade_size, bkt_min_score, bkt_sl_atrp, bkt_tp_atrp, bkt_sl_percentage, bkt_tp_percentage, bkt_direction, bkt_with_trend, bkt_kill_trade, bkt_equity, bkt_score_type, bkt_sma_length, bkt_cooldown_time "
    _sql += " , bkt_trend_sma_length,	bkt_btc_threshold, bkt_commission, bkt_time1, bkt_time2, bkt_time3, bkt_daytrading from backtests where bkt_id = :id"
    res = connection_global.execute(text(_sql), id = _bkt_id).one()
    
    coins = []
    if res['bkt_coin'] != 0:
        tab = json.loads(res['bkt_coins'])
        for t in tab:
            coins.append(int(t))
    else:
        res_coins = connection_global.execute("select coin_id from coinlist where coin_name <> 'BTCUSDT'").fetchall()
        for r in res_coins:
            coins.append(r['coin_id'])
    
    coins_per_core = round(len(coins)/NUM_CORES)
    
    futures = []
    _part_of_multi = 0
    if len(coins)>1:
        _part_of_multi = 1
    all_tests = len(coins)
    with concurrent.futures.ProcessPoolExecutor(NUM_CORES) as executor:
        if coins_per_core>0:
            for i in range(NUM_CORES - 1):
                new_future = executor.submit(
                    run_single_core, # Function to perform
                    # v Arguments v
                    res=res,
                    coins = coins[:coins_per_core],
                    part_of_multi= _part_of_multi
                    
                )
                futures.append(new_future)
                coins = coins[coins_per_core:]

        futures.append(
            executor.submit(
                run_single_core,
                res=res, coins=coins,part_of_multi=_part_of_multi
            )
        )
    
    concurrent.futures.wait(futures)
    
    for f in futures:
        print(f.result())
    
    if _test_10_zero_in_row>=10:
        print("10 or more with no data")
        connection_global.execute(text("update backtests set bkt_status = 'E', bkt_error='Ten or more coins in row with no data' where bkt_id = :id"), id = _bkt_id)
        connection_global.execute(text("delete from backtest_tmp where bkt_id = :_id"), _id = _bkt_id)
        return 
    connection_global.execute(text("UPDATE backtests set bkt_progress = 100 where bkt_id = :id"), id = _bkt_id)        
   
    

            
    if _part_of_multi == 1:
        connection_global.execute(text("update backtests set backtests.bkt_trades= ( SELECT  CONCAT('[',GROUP_CONCAT(result SEPARATOR ', '),']')  FROM backtest_tmp  where backtest_tmp.bkt_id = backtests.bkt_id and backtest_tmp.type='T') where backtests.bkt_id = :_id "), _id = _bkt_id)
        connection_global.execute(text("update backtests set backtests.bkt_summary= ( SELECT  CONCAT('[',GROUP_CONCAT(result SEPARATOR ', '),']')  FROM backtest_tmp  where backtest_tmp.bkt_id = backtests.bkt_id and backtest_tmp.type='O') where backtests.bkt_id = :_id "), _id = _bkt_id)
    else:
        connection_global.execute(text("update backtests set backtests.bkt_trades= ( SELECT  result  FROM backtest_tmp  where backtest_tmp.bkt_id = backtests.bkt_id  and backtest_tmp.type='T' LIMIT 1) where backtests.bkt_id = :_id "), _id = _bkt_id)
        
    
    connection_global.execute(text("delete from backtest_tmp where bkt_id = :_id"), _id = _bkt_id)

   
    
    _webpage = '<h4>No Data</h4>'
    
    if _part_of_multi == 1:
        _sql = "update backtests set bkt_status = 'Z' "
        _sql += ", bkt_sharpe_ratio = :bkt_sharpe_ratio"
        _sql += ", bkt_proft_factor = :bkt_profit_factor"
        _sql += ", bkt_win_rate = :bkt_win_rate"
        _sql += ", bkt_win_amount = :bkt_win_amount, bkt_webpage = :webpage"    
        _sql += " where bkt_id = :bkt_id"
        
        _bkt_win_rate = -1
        _bkt_sharpe_ratio = -1
        _bkt_profit_factor = -1
        _bkt_win_amount = -1
        connection_global.execute(text(_sql),  bkt_id = _bkt_id,bkt_win_rate = _bkt_win_rate, bkt_sharpe_ratio = _bkt_sharpe_ratio ,bkt_profit_factor = _bkt_profit_factor,  bkt_win_amount = _bkt_win_amount, webpage = _webpage )
    
    
    
    

if __name__ == "__main__":
    NUM_CORES = cpu_count()
    print("Running on ", NUM_CORES, " cores")
    _bkt_id = 0    
    main()
    connection_global.close()
    
    
