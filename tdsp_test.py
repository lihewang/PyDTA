# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
import pyximport; pyximport.install(reload_support=True)

import yaml as ym
import pandas as pd
import timeit
from tqdm import tqdm
from multiprocessing import Process, Queue
from worker import worker

       
if __name__ == "__main__":
    print('--- start ---') 
    processes = []
    with open(r'..\Data\control.yaml') as file:
        par = ym.full_load(file)
    
    #start multiprocessors
    q = Queue()
    for i in range(par['num_processor']):
        p=worker(q)
        p.start()
        processes.append(p)  
    
    #Read trip table file
    t1 = timeit.default_timer()
    trips = pd.read_csv(par['trip_file'], skiprows=1)
    trips.set_index(['ORG','DES','PERIOD'], inplace=True)    
    trips = pd.DataFrame(trips.stack())
    trips.reset_index(inplace=True)
    trips.columns = ['I','J','period','class','trip']   
    trips.drop(trips[trips['trip'] == 0].index, inplace = True)
    trips.drop(trips[trips['I'] == trips['J']].index, inplace = True)
    
    sp_task = trips[:10].to_numpy()
    
    t2 = timeit.default_timer()
    print(f"Read trips: {t2 - t1:0.2f} seconds")
    

        
    print('--- end of run ---')
