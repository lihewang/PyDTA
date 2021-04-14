# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 2021
@author: Lihe Wang  

Read node and link csv files and create shared memory for multiprocessing
"""
import numpy as np
import pandas as pd
from multiprocessing import shared_memory
import snowflake.connector

def read(node, link):
    ctx = snowflake.connector.connect(
        user='lihewang',
        password='*******',
        account='eya42508.us-east-1',
        warehouse='COMPUTE_WH',
        database='TEST',
        schema='PUBLIC'
        )
    cur = ctx.cursor()

    sql = "select * from " + node
    cur.execute(sql)
    df_node = cur.fetch_pandas_all()

    sql = "select * from " + link
    cur.execute(sql)
    df_link = cur.fetch_pandas_all()

    ndarr_node = df_node.to_numpy()
    ndarr_link = df_link.to_numpy()

    shm_node = shared_memory.SharedMemory(name='shared_node', create=True, size=ndarr_node.nbytes)
    shm_link = shared_memory.SharedMemory(name='shared_link', create=True, size=ndarr_link.nbytes)

    node = np.ndarray(ndarr_node.shape, dtype=ndarr_node.dtype, buffer=shm_node.buf)
    link = np.ndarray(ndarr_link.shape, dtype=ndarr_link.dtype, buffer=shm_link.buf)

    node[:] = ndarr_node[:]
    link[:] = ndarr_link[:]

    return [shm_node, shm_link], [ndarr_node.shape, ndarr_node.dtype, ndarr_link.shape, ndarr_link.dtype, df_node.columns, df_link.columns]
