from sqlalchemy.sql import text
import sqlalchemy
import time
import subprocess, signal
import os
_current_id_for_check =-1
def kill_proc():
    p = subprocess.Popen(['ps', '-A','-f'], stdout=subprocess.PIPE)
    out, err = p.communicate()
    for line in out.splitlines():
            
            line = line.decode("utf-8")
            if '/app/run_optimize.py' in line:            
                pid = int(line.split()[1])                
                print("PID = ", pid)                
                os.kill(pid, signal.SIGKILL)


        
while True:
    try:
        _db_link = "mysql://root:"+os.environ['DB_PASSWORD']+"@"+os.environ['DB_HOST']+"/"+os.environ['DB_DATABASE']
        engine = sqlalchemy.create_engine(_db_link)
        connection_global = engine.connect()
        if _current_id_for_check != -1:
            _rec = connection_global.execute(text("SELECT opt_status from optimizes where opt_id =:d"), d = _current_id_for_check).first()
            if _rec['opt_status'] == 'B':
                kill_proc()
                _current_id_for_check = -1 
        else:       
            _rec = connection_global.execute(text("SELECT opt_id from optimizes where opt_status = 'P'")).first()
            if _rec != None:
                _current_id_for_check = _rec['opt_id']
                print("Found test for observe: ", _current_id_for_check)
                
        _rec = connection_global.execute("select cnf_restart_optimiser from config").first()
        if _rec['cnf_restart_optimiser']  == 1:
            connection_global.execute("update config set cnf_restart_optimiser = 0")
            connection_global.execute("update optimizes set opt_status = 'B' where opt_status='P'")
            connection_global.execute("update optimizes set opt_status = 'B' where opt_status='O'")
            _current_id_for_check = 0
            kill_proc()
            
        connection_global.close()
        time.sleep(0.2)
    except BaseException as e:
        _current_id_for_check = -1
        print('An exception occurred: {}'.format(e))
       


