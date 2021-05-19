"""
Utility program, not used in the model
"""
import numpy as np
import pandas as pd
import snowflake.connector

table_name = 'VOLUME_TOTAL'
ctx = snowflake.connector.connect(
    user='lihewang',
    password='Ui123456',
    account='eya42508.us-east-1',
    warehouse='COMPUTE_WH',
    database='TEST',
    schema='PUBLIC'
    )
cur = ctx.cursor()

sql = "select * from " + table_name
cur.execute(sql)
df_vol = cur.fetch_pandas_all()
df_vol.to_csv('../Output/vol_SUBAREA_TOTAL_i1.csv', index=False)