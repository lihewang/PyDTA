# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
import numpy as np
import pandas as pd
from multiprocessing import shared_memory
def read(nodefile, linkfile):
    df_node = pd.read_csv(nodefile)
    df_link = pd.read_csv(linkfile)

    ndarr_node = df_node.to_numpy()
    ndarr_link = df_link.to_numpy()

    shm_node = shared_memory.SharedMemory(name='shared_node', create=True, size=ndarr_node.nbytes)
    shm_link = shared_memory.SharedMemory(name='shared_link', create=True, size=ndarr_link.nbytes)

    node = np.ndarray(ndarr_node.shape, dtype=ndarr_node.dtype, buffer=shm_node.buf)
    link = np.ndarray(ndarr_link.shape, dtype=ndarr_link.dtype, buffer=shm_link.buf)

    node[:] = ndarr_node[:]
    link[:] = ndarr_link[:]

    return shm_node, shm_link, ndarr_node.shape, ndarr_node.dtype, ndarr_link.shape, ndarr_link.dtype