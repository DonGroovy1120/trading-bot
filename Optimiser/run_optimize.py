import numpy as np
import json
from sqlalchemy.sql import text
import pandas as pd
import time
import sqlalchemy
import sys
import os
import math
import optuna
from backtesting import Backtest, Strategy

sys.path.append('/app/backtesting')
from run_backtest import DivergenceStrategy
from run_backtest import _data_period
from run_backtest import _prepare_data_for_signals
INVALID = -1e300

_db_link = "mysql://root:"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_DATABASE']
engine = sqlalchemy.create_engine(_db_link)
connection_global = engine.connect()
#global varaibles
_commission = 0
all_params = []
all_price_data = []
query_flag = 0
max_factor = ''
_opt_id = 0
_break_check_counter = 0
_walk_forward = '0'
def report_error(st):
    print("ERROR: "+st)    
    connection_global.close()
    sys.exit()
def toMinutes(st):
    tab = st.split(':')
    return int(tab[0])*60+int(tab[1])
def get_range(tab, param):
    _sc_type = ['S','Z','P','B','V','I','D']
    _atrp_sl_tp = [0,1440,480,240,120,60,30,15,10,5,3,1]
    _trade_type = [0,1,-1]
    _with_trend = [0,1,2]
    _day = [0,1]
    if 'step__'+param in tab:
        if len(tab['min__'+param].split(':')) == 2:
            _min = toMinutes(tab['min__'+param])
            _max = toMinutes(tab['max__'+param])
            _step = toMinutes(tab['step__'+param])
            _t =  np.arange(_min, _max, _step)
            if len(_t)>0:
                if (_t[-1] != _max):
                    _t = _t.tolist() + [_max]
            _tz = []
            for _p in _t:
                _h = str(math.floor(_p/60))
                _m = str(_p%60)
                
                if (len(_h) == 1):
                    _h = '0'+_h
                if (len(_m) == 1):
                    _m = '0'+_m
                _tz.append(_h+':'+_m)
            
            if len(_tz) == 1:
                return _tz[0]
            else:
                return _tz
            
            
        else:
            _min = float(tab['min__'+param])
            _max = float(tab['max__'+param])
            _step  = float(tab['step__'+param])
                    
            _t =  np.arange(_min, _max, _step)
            if len(_t)>0:
                if (_t[-1] != _max):
                    _t = _t.tolist() + [_max]
            if len(_t) == 1:
                return _t[0]
            else:
                return _t
    else:
        
        res = []
        _min = None
        _max = None

        if param == 'score_type':
            res = _sc_type.copy()
        if param == 'atrp_tpsl':
            res = _atrp_sl_tp.copy()
        if param == 'trades_col':
            res = _trade_type.copy()
        if param == 'trade_with_trend':
            res = _with_trend.copy()
        _min = tab['min__'+param]
        _max = tab['max__'+param]
        if param != 'score_type':
            _min = int(tab['min__'+param])
            _max = int(tab['max__'+param])        
        if param in ['day_mon', 'day_tue', 'day_wed', 'day_thu', 'day_fri','day_sat','day_sun']:
            res = _day.copy()
        if len(res) == 0:
            report_error("Wrong select parameter")
        _ind_min = res.index(_min)
        _ind_max = res.index(_max)
        
        if _ind_min>_ind_max:
            _temp = _ind_min
            _ind_min = _ind_max
            _ind_max = _temp        
        return res[_ind_min:_ind_max+1]

def trading_strategy(params):
    
    print("Starting new optimise trial")
    global all_price_data
    global _commission
    global max_factor
    global _opt_id    
    global _walk_forward
    global _break_check_counter
    #check if not break
        
        
    
    _fields = ['Return [%]','Sharpe Ratio','# Trades','Win Rate [%]','Profit Factor','SQN']
    _found_trades = False
    _result = 0
    _data_result = {}
    for _data in all_price_data:        
            print("preparing new data for optimise")
            df =  _prepare_data_for_signals(int(params['score_period']), int(params['atrp_tpsl']), _data['df'].copy(), params['score_type'], 14, int(params['sma_length']), int(params['trend_sma_length']), 0 , 0)
            dfs = []
            _recs = len(df)
            _wks = _walk_forward.split(':')
            if len(_wks) == 2:
                _recs = int((float(_wks[0])/((float(_wks[0]))+(float(_wks[1]))))*_recs)
                dfs.append(df[:_recs])
                dfs.append(df[_recs:])
            if (len(dfs)==0):
                dfs.append(df)
            first = 0
            _outputs_all = {}
            for _d in dfs:
                print("running new backtest with ", str(len(_d)), " records.")
                print("Running with commission: ", _commission)
                
                bt = Backtest(_d,DivergenceStrategy,
                                    cash = 1000000,
                                    commission = _commission/100,
                                    exclusive_orders = True
                                    )
                _ret = INVALID
                _days = []
                _tab = ['day_mon', 'day_tue','day_wed','day_thu','day_fri','day_sat','day_sun']    
                _cnt = 1
                for _t in _tab:
                    if int(params[_t]) == 1:                        
                        _days.append(str(_cnt))
                    _cnt += 1
                
                _output = []
                
                try:
                    _output = bt.run(trade_size = 1,
                                        min_score = float(params['min_score']),
                                        atrp_tf = int(params['atrp_tpsl']),
                                        sl_atrp = float(params['atrp_slpercent']),
                                        tp_atrp = float(params['atrp_tppercent']),
                                        sl_percentage = float(params['slfpercentlevel']),
                                        tp_percentage = float(params['tppercentlevel']),
                                        direction = int(params['trades_col']),
                                        with_trend = int(params['trade_with_trend']),
                                        bkt_id = 0,
                                        kill_trade = float(params['killtrade']),
                                        score_type = params['score_type'],
                                        cooldown_time = float(params['cooldown_period']),
                                        part_of_multi = 0,
                                        btc_push_threshold = float(params['btc_threshold']),
                                        coin_id = _data['coin'],
                                        records_amouont = len(df),
                                        time1 = params['time1'],
                                        time2 = params['time2'],
                                        time3 = params['time3'],
                                        days = json.dumps(_days),
                                        connection = connection_global)
                except BaseException as e:
                    print("Error on backtest: ",e)
                print("Backtest done")
                res = {}
                if (first == 0):                    
                    if math.isnan(_output[max_factor]) == False:
                        _result += _output[max_factor]
                        _found_trades = True
    

                
                for _f in _fields:
                    if math.isnan(_output[_f]):
                        res[_f] = "NaN"
                    else:
                        res[_f] = _output[_f]
                print("Preparing outputs")
                _outputs_all["res_"+str(first)] = res
                    
                first = 1
                print("Update progress")
                connection_global.execute(text("update optimizes set opt_progress = opt_progress+1 where opt_id = :id"), id = _opt_id)
                print("updated")
            _data_result[_data['coin']] = _outputs_all
            print("finising trial")

       
    _data_result['params'] = params
    if _found_trades:        
        print("Insert output informatation")
        connection_global.execute(text("INSERT INTO optimizes_tmp (opt_id, result) VALUES (:id, :res)"), id = _opt_id, res = json.dumps(_data_result))
        print("Result: ",str(_result), " data: "+str(len(all_price_data)))
        return _result/len(all_price_data)  
    else:
        return INVALID
_old_params = []
def objective(trial):
    global _old_params
    
    new_params = {}
    print("Preparing params for new trial")
    for k in all_params:                
        if (k['val'] != ''):
            new_params[k['param']] = trial.suggest_categorical(k['param'],k['val'])
        else:
            new_params[k['param']] = ''
        
    #optimise check if done already
    _the_same = False
    _r = 0
    for _op in _old_params:
        _the_same = True
        for k in new_params.keys():
            if new_params[k] != _op[k]:
                _the_same = False                
                break
        if _the_same:
            _r = _op['res']
            break
    if _the_same:
        print("Skip backtest")
        return -_r
        
    
    print("Prepared params")
    print("Starting strategy")
    res = trading_strategy(new_params)
    print("Strategy done with result: ",str(-res))
    
    rec = new_params.copy()
    rec['res'] = res
    _old_params.append(rec)
    return -res


def main():
    global all_params
    global all_price_data
    global _commission
    global max_factor
    global _opt_id
    global _walk_forward
    res = connection_global.execute("select min(opt_id) id from optimizes where opt_status = 'O' ").first()
    if res['id'] == None:
        print("no waiting optimizes")
        return
    _opt_id = res['id']
    
    connection_global.execute(text("update optimizes set opt_status = 'P', opt_progress = 0 where opt_id = :id"), id = _opt_id)
    
    connection_global.execute(text("delete from optimizes_tmp where opt_id = :id"), id = _opt_id)
    try:
    
            res = connection_global.execute(text("select * from optimizes where opt_id = :id"), id = _opt_id).first()
            tab = json.loads(res['opt_settings'])
            _commission = 0
            if 'commission' in tab:
                _commission = float(tab['commission'])
            _static_params = []
            _dynamic_params = []
            for k in tab.keys():
                params = k.split("switch__")
                if len(params) == 2:
                    _param_name = params[1]                        
                    if tab[k]:
                        _dynamic_params.append({"param":_param_name, "val":get_range(tab, _param_name)})            
                    else:
                        if 'step__'+_param_name not in tab:
                            _static_params.append({"param":_param_name, "val":[tab[_param_name]]})
                        else:
                            if (tab[_param_name] == ''):
                                _static_params.append({"param":_param_name, "val":''})
                            else:
                                if len(tab[_param_name].split(':')) == 2:
                                    _static_params.append({"param":_param_name, "val":[tab[_param_name]]})
                                else:
                                    _static_params.append({"param":_param_name, "val":[float(tab[_param_name])]})
            all_params = _dynamic_params + _static_params
            
            
            max_factor = tab['max_factor']
            _walk_forward = res['opt_walk_forward']
            
            for c in json.loads(res['opt_coins']):        
                _coin_id = int(c)
                _data =  _data_period(_coin_id,res['opt_from'],res['opt_to'],connection_global)
                all_price_data.append({'coin':_coin_id, "df":_data})
            
            
            print("Create optune study")
            study = optuna.create_study()
            study.optimize(objective, n_trials=int(tab['optimise_steps']), n_jobs = 1)
            print("All Optimizes done, updating")
            connection_global.execute(text("update optimizes set optimizes.opt_result= ( SELECT  CONCAT('[',GROUP_CONCAT(result SEPARATOR ', '),']')  FROM optimizes_tmp  where optimizes_tmp.opt_id = optimizes.opt_id ) where optimizes.opt_id = :_id "), _id = _opt_id)
            print("Output updated")
            connection_global.execute(text("update optimizes set opt_status = 'Z' where opt_id = :id"), id = _opt_id)
            print("status changed")
            connection_global.execute(text("delete from optimizes_tmp where opt_id  = :id"), id = _opt_id)
            print("Cleaned temp")
            time.sleep(1)
       
    except BaseException as e:
        connection_global.execute(text("delete from optimizes_tmp where opt_id  = :id"), id = _opt_id)
        connection_global.execute(text("update optimizes set opt_status = 'E', opt_progress = 0, opt_error = :error where opt_id = :id"), id = _opt_id, error = str(e))
        print('An exception occurred: {}'.format(e))
            

        
    
    
    



    


if __name__ == "__main__":    
    main()
    print("End")
    connection_global.close()
    print("Connection closed")
    