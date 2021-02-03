# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
import pyximport; pyximport.install(reload_support=True)
import tdsp
import yaml as ym
import pandas as pd
import timeit
from tqdm import tqdm
import time
import multiprocessing as mp

def load(sp_task):
    num_zones = 4584
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)
        
    nodefile = par['node_file']
    linkfile = par['link_file']
    
    sp = tdsp.tdsp(nodefile, linkfile, num_zones) 
    time.sleep(1)
    
    t1 = timeit.default_timer()    
    for t in tqdm(sp_task): 
        sp.build(t)
        sp.trace(t)
        
    t2 = timeit.default_timer()
    print(f"Run time for path build is {t2 - t1:0.6f} seconds")
        
if __name__ == "__main__":
    print('--- start ---') 
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)
      
    trips = pd.read_csv(par['trip_file'], skiprows=1)
    trips.set_index(['ORG','DES','PERIOD'], inplace=True)
    
    # trips = pd.read_csv(par['trip_file'])
    # trips.set_index(['I','J','period'], inplace=True)  
    
    trips = pd.DataFrame(trips.stack())
    trips.reset_index(inplace=True)
    trips.columns = ['I','J','period','class','trip']   
    trips.drop(trips[trips['trip'] == 0].index, inplace = True)
    trips.drop(trips[trips['I'] == trips['J']].index, inplace = True)
    
    sp_task = trips[:100].to_numpy()
    # load(sp_task)
    
    with mp.Pool(par['num_processor']) as process:        
        process.map(load, sp_task)
    
    print('--- end of run ---')
