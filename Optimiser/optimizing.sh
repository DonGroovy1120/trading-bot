#!/bin/sh

while [ 1==1 ]
do

    T=`ps -A -f | grep -v grep | grep "optimise_break_check.py" | wc -l`
    if [ "$T" = "0" ]
    then
  
        echo  ">>> RUN break checker <<<<"
        python /app/optimise_break_check.py &
    fi

    
    echo "running optimizing"
    python /app/run_optimize.py
    sleep 2

done
