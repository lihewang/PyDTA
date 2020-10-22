# -*- coding: utf-8 -*-
"""
Created on Fri Oct  9 16:25:50 2020

@author: lihe.wang
"""

import yaml as ym
import pandas as pd
import multiprocessing as mp

def load(x):
    return x*x

if __name__ == "__main__":
    #Read control file
    with open(r'Data\control.yaml') as file:
        par = ym.full_load(file)
        
    link = pd.read_csv(par['link_file'])
    node = pd.read_csv(par['node_file'])
    #trip = pd.read_csv(par['trip_file'], skiprows=1)

    with mp.Pool(par['num_processor']) as process:
        print(process.map(load, [1, 2, 3]))