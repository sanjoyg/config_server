#!/bin/sh
pid=`/bin/ps -eaf|/bin/grep "python config"|/bin/grep -v grep|/usr/bin/awk '{print $2}'`
if [ -z $pid ]
then
  echo "configserver not running!"
else
  /bin/kill -2 $pid
fi


