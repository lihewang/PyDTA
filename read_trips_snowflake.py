import numpy as np
import pandas as pd
import snowflake.connector

def read_trips(trip):
    ctx = snowflake.connector.connect(
        user='lihewang',
        password='****',
        account='eya42508.us-east-1',
        warehouse='COMPUTE_WH',
        database='TEST',
        schema='PUBLIC'
        )
    cur = ctx.cursor()

    sql = "select * from " + trip
    cur.execute(sql)
    df_trips = cur.fetch_pandas_all()

    return df_trips
