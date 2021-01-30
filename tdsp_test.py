# -*- coding: utf-8 -*-
"""
Created on Wed Jan 27 10:45:13 2021

@author: lihe.wang
"""
import pyximport; pyximport.install(reload_support=True)
import tdsp
import pandas as pd
import timeit

if __name__ == "__main__":
    print('--- start ---') 
    nodefile = "../Data/Regional/NODE.CSV"
    linkfile = "../Data/Regional/LINK.CSV"
    
    sp_task = pd.Series({'O':2, 'D':4344, 'TS':12, 'Type':0, 'Cost':0})
    sp = tdsp.tdsp(nodefile, linkfile) 
    t1 = timeit.default_timer()       
    sp.build(sp_task)
    t2 = timeit.default_timer()
    
    sp.trace(sp_task)
    print(f"Run time for path build is {t2 - t1:0.6f} seconds")
    print('--- end of run ---')
