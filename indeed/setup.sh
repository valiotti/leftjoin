#!/bin/bash
nohup python3.7 $(/home/ubuntu/leftjoin/indeed/main.py) 2>>/home/ubuntu/leftjoin/indeed/error.log 1>/dev/null &
