#!/bin/bash
export PATH=$PATH:/sbin/:/usr/local/bin/
if [ $# -ne 1 ]; then
  echo "param error"
  exit 1
fi

message=$1

alarm_time=$(date +"%Y-%m-%d %H:%M:%S")
host=$(ifconfig -a|grep inet|grep -v 127.0.0.1|grep -v inet6 | awk '{print $2}' | tr -d "addr:")
message="From ${host}: ${message}"

#curl -d "data=$data&requestType=json" 'http://yourapi.com/alarm'

if [ $? -ne 0 ]; then
  echo "curl failed"
  exit 1
fi
