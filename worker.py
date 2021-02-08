# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 15:45:34 2021

@author: lihe.wang
"""

import yaml as ym
import tdsp

from multiprocessing import Process

class worker(Process):
    
    def __init__(self, job_queue, r_queue, i):
        super(worker, self).__init__()
        with open(r'..\Data\control.yaml') as file:
            par = ym.full_load(file)
            
        self.num_zones = par['num_processor']
        self.node_file = par['node_file']
        self.link_file = par['link_file']

        self.job_queue = job_queue
        self.r_queue = r_queue
        
        
    def run(self):

        sp = tdsp.tdsp(self.node_file, self.link_file, self.num_zones)
        while True :
            task = self.job_queue.get()
        
            sp.build(task) 
            time, dist = sp.trace(task)
            self.r_queue.put(sp.trace(task))                

            self.job_queue.task_done()

