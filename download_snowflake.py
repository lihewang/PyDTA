import numpy as np
import pandas as pd
import snowflake.connector

table_name = 'Volume'
ctx = snowflake.connector.connect(
    user='lihewang',
    password='Ui123456',
    account='eya42508.us-east-1',
    warehouse='COMPUTE_WH',
    database='REGION_MODEL',
    schema='PUBLIC'
    )
cur = ctx.cursor()

sql = "select * from " + table_name
cur.execute(sql)
df_vol = cur.fetch_pandas_all()
df_vol.to_csv('Output/vol_sf1.csv', index=False)