#!/bin/sh

while [ 1==1 ]
do
    echo "running backtest"
    python /app/run_backtest.py
    sleep 2

done
