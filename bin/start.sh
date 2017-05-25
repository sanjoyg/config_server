#!/bin/sh
INSTALL_DIR=/c503/config_server/bin
cd $INSTALL_DIR
/usr/bin/python configserver.py -s pitabread.local -n 1883 -c /c503/sensor_configs -t c503/system/sensorconf -r c503/system/NODE_ID/sensorconf -l /var/log/configserver.log 
