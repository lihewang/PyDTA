# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 10:33:13 2021

@author: lihe.wang
"""

#!/usr/bin/env python
import snowflake.connector

file_path = 'file:///Users/lihe.wang/Documents/PyDTA/Data/TSM/'
data_base_name = 'TSM_MODEL'
# NODE
node_table_name = 'NODE'
node_field = 'N INTEGER, X FLOAT, Y FLOAT, DTA_TYPE INTEGER, DNGRP INTEGER'
node_format = 'UTIL.PUBLIC.CSV_FORMAT'
# LINK
link_table_name = 'LINK'
link_field = 'A INTEGER,B INTEGER,DISTANCE FLOAT,FTYPE INTEGER,AREATYPE INTEGER,COUNT STRING,' \
        'ALPHA FLOAT,BETA FLOAT,TOLL FLOAT,NUMSTOPS FLOAT,NUMYIELD FLOAT,NUMOTHER FLOAT,NUMSIGNALS FLOAT,IMPFAC FLOAT,DELAY_FLAG INTEGER,TOLLSEGNUM INTEGER,TOLL_POLICY INTEGER,' \
        'FNAME INTEGER,ANAME INTEGER,POSTSPEED INTEGER,FFSPEED INTEGER,CAPACITY INTEGER,EDIT INTEGER,CAV INTEGER'
link_format = 'UTIL.PUBLIC.CSV_FORMAT'
# TRIP
trip_table_name = 'trip_table_cav_50pct'
trip_field = 'period INTEGER,O INTEGER,D INTEGER,res_H FLOAT,res_L FLOAT,res_M FLOAT,vis FLOAT,trk FLOAT,res_Ha FLOAT,res_La FLOAT,res_Ma FLOAT,visa FLOAT,trka FLOAT'
trip_format = 'UTIL.PUBLIC.CSV_SKIP_FORMAT'


def upload(cs, file_path, table_name, field, file_format):
    try:
        cs.execute("DROP TABLE if exists " + table_name)
        cs.execute("CREATE TABLE " + data_base_name + ".PUBLIC." + table_name + " (" + field + ")")
        cs.execute("PUT " + file_path + table_name + ".csv @%" + table_name)
        cs.execute("COPY INTO " + table_name + " FILE_FORMAT = " + file_format)
        one_row = cs.fetchone()
        print('upload successfully to ' + one_row[0])
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)

# upload file
ctx = snowflake.connector.connect(
    user='lihewang',
    password='Ui123456',
    account='eya42508.us-east-1',
    warehouse='COMPUTE_WH',
    database=data_base_name,
    schema='PUBLIC'
    )
cs = ctx.cursor()

upload(cs, file_path, node_table_name, node_field, node_format)
upload(cs, file_path, link_table_name, link_field, link_format)
upload(cs, file_path, trip_table_name, trip_field, trip_format)

cs.close()
ctx.close()