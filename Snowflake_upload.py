#!/usr/bin/env python
import snowflake.connector

file_path = 'file:///Users/lihe.wang/Documents/PyDTA/Data/'
# NODE
node_table_name = 'NODE'
node_field = 'N INTEGER, X INTEGER, Y INTEGER, DTA_TYPE INTEGER, DNGRP INTEGER'
node_format = 'UTIL.PUBLIC.CSV_FORMAT'
# LINK
link_table_name = 'LINK'
link_field = 'A INTEGER,B INTEGER,DISTANCE FLOAT,CAPACITY INTEGER,CAPLN INTEGER,FTYPE INTEGER,' \
        'FFSPEED FLOAT,ALPHA FLOAT,BETA FLOAT,IMPFAC FLOAT,TOLLSEGNUM INTEGER,TOLL_POLICY INTEGER,' \
        'TOLL FLOAT,SEG_DISTANCE FLOAT,Truck INTEGER,DELAY_FLAG INTEGER,CAV INTEGER'
link_format = 'UTIL.PUBLIC.CSV_FORMAT'
# TRIP
trip_table_name = 'Subarea_Trips'
trip_field = 'ORG INTEGER,DES INTEGER,period INTEGER,AUTOVOT1 FLOAT,AUTOVOT2 FLOAT,AUTOVOT3 FLOAT,AUTOVOT4 FLOAT,AUTOVOT5 FLOAT,Truck FLOAT'
trip_format = 'UTIL.PUBLIC.CSV_SKIP_FORMAT'


def upload(cs, file_path, table_name, field, file_format):
    try:
        cs.execute("DROP TABLE if exists " + table_name)
        cs.execute("CREATE TABLE TEST.PUBLIC." + table_name + " (" + field + ")")
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
    database='TEST',
    schema='PUBLIC'
    )
cs = ctx.cursor()

upload(cs, file_path, node_table_name, node_field, node_format)
upload(cs, file_path, link_table_name, link_field, link_format)
upload(cs, file_path, trip_table_name, trip_field, trip_format)

cs.close()
ctx.close()