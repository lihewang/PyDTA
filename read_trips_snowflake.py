import numpy as np
import pandas as pd
import snowflake.connector
#" unpivot(trip for class in (AUTOVOT1, AUTOVOT2, AUTOVOT3, AUTOVOT4, AUTOVOT5, TRUCK))" + \
def read_trips(db, trip_table):
    ctx = snowflake.connector.connect(
        user='lihewang',
        password='Ui123456',
        account='eya42508.us-east-1',
        warehouse='COMPUTE_WH',
        database=db,
        schema='PUBLIC'
        )
    cur = ctx.cursor()

    sql = "select * from " + trip_table + \
            " unpivot(trip for class in (RES_H, RES_L, RES_M, VIS, TRK, RES_HA, RES_LA, RES_MA, VISA, TRKA))" + \
            " where O <> D and trip <> 0"
    cur.execute(sql)
    df_trips = cur.fetch_pandas_all()
    
    return df_trips