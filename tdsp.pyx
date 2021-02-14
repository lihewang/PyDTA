# -*- coding: utf-8 -*-
#cython: language_level=3
"""
Created on Wed Jan 27 09:13:49 2021

@author: lihe.wang
"""
import logging
from cymem.cymem cimport Pool
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from libc.string cimport memcpy
cimport typedef as td
import pandas as pd
import numpy as np
import timeit
import time
cimport heap as hp


cdef class tdsp:
    cdef td.node* nodes
    cdef td.link* links
    cdef Pool mem
    cdef object node_index
    cdef int num_nodes 
    cdef int num_zones
    cdef int num_links
    cdef int num_time_steps
    def __init__(self, nodefile, linkfile, num_zones, num_time_steps):
        log = logging.getLogger(__name__)
        
        t1 = timeit.default_timer()        
        cdef int i, link_index
        cdef double t
        self.mem = Pool()
        self.num_zones = num_zones
        self.num_time_steps = num_time_steps
        
        df_node = pd.read_csv(nodefile)
        df_link = pd.read_csv(linkfile)
        
        self.num_nodes = len(df_node)
        self.num_links = len(df_link)
        self.nodes = <td.node*>self.mem.alloc(self.num_nodes, sizeof(td.node))
        self.links = <td.link*>self.mem.alloc(self.num_links, sizeof(td.link))
        
        #--- create node struct ---
        #use node index internally to identify nodes
        log.info('create node struct')
        self.node_index = pd.Series(df_node.index, index=df_node['N'], name='nd_index')
        #link's a_node index
        index_a = pd.Series(self.node_index[df_link['A']].values, index=df_link.index)
        #link index of a_node
        df_index_a = pd.DataFrame({'anode':index_a, 'link':index_a.index})
        df_next_links = df_index_a.groupby('anode')['link'].apply(list)    

        for index, row in df_node.iterrows(): 
            self.nodes[index].ni = index
            self.nodes[index].n = row['N']

            if index in df_next_links:
                list_next_links = df_next_links[index]
            else:
                list_next_links = []
            
            self.nodes[index].num_nxlinks = len(list_next_links)
            self.nodes[index].nxt_links = <td.link**>self.mem.alloc(len(list_next_links), sizeof(td.link*))
            
            i = 0
            for link_index in list_next_links:            
                self.nodes[index].nxt_links[i] = &self.links[link_index]
                i += 1
        
        t2 = timeit.default_timer()  
        log.info(f"Run time for node struct is {t2 - t1:0.2f} seconds")
            
        t1 = timeit.default_timer()       
        # --- create link struct ---  
        log.info('create link struct')
        #ffspeed can't be zero        
        df_link['FFSPEED'] = df_link['FFSPEED'].replace(0, 25)  
        # loop links
        for index, row in df_link.iterrows():
            self.links[index].ai = self.node_index[row['A']]
            self.links[index].bi = self.node_index[row['B']]
            self.links[index].a = row['A']
            self.links[index].b = row['B']

            self.links[index].dist = row['DISTANCE']
            self.links[index].ffspd = row['FFSPEED']
            #time for time steps
            self.links[index].time = <double*>self.mem.alloc(num_time_steps, sizeof(double))
            self.links[index].vol = <double*>self.mem.alloc(num_time_steps, sizeof(double))
            t = row['DISTANCE']/row['FFSPEED']*60       
            for i in range(num_time_steps):
                self.links[index].time[i] = t
                self.links[index].vol[i] = 0
                
        t2 = timeit.default_timer()  
        log.info(f"Run time for link struct is {t2 - t1:0.2f} seconds")
        
    #build path    
    cpdef build(self, sp_task):
        # print('build path from ' + str(sp_task[0]) + ' to ' + str(sp_task[1]) + ' ts ' + str(sp_task[2]))
        cdef hp.heap my_heap = hp.heap(self.num_nodes+1)
        cdef int o_node_index, d_node_index, start_ts, curr_ts, i, j
        cdef td.node *top_node
        cdef td.node *b_node
        cdef td.node *nd
        cdef td.link *nxlink
        cdef double link_time, imp, new_imp, max_imp = 99999.9
        
        o_node_index = self.node_index[sp_task[0]]
        d_node_index = self.node_index[sp_task[1]]  
        start_ts = sp_task[2]
        #initialize
        for j in range(self.num_nodes):
            self.nodes[j].imp = max_imp
            self.nodes[j].popped = 0
            # self.nodes[j].time = 0
            # self.nodes[j].dist = 0
            # self.nodes[j].toll = 0
            self.nodes[j].parent = NULL
            
        self.nodes[o_node_index].imp = 0
        self.nodes[o_node_index].parent = NULL
        my_heap.insert(&self.nodes[o_node_index])

        while not my_heap.is_empty():
            top_node = my_heap.pop()
            top_node.popped = 1
            # print('--popped node ' + str(top_node.n) + ' imp ' + str(top_node.imp))
            #zones can't be transpassed    
            if top_node.n <= self.num_zones and top_node.ni != o_node_index:
                continue
            
            #Calculate time step
            curr_ts = int(top_node.node_time/15) + start_ts
            if curr_ts > 96:
                curr_ts -= 96
            
            #Next links index
            # for i in range(1):
            for i in range(top_node.num_nxlinks):
                nxlink = top_node.nxt_links[i]
                b_node = &self.nodes[nxlink.bi]
                # print('B node ' + str(b_node.n))
                # time.sleep(1)
                #Path can't be backward
                if b_node == top_node or b_node.popped == 1:
                    continue
                
                link_time = nxlink.time[curr_ts]
                imp = link_time #+ 0.2 * nxlink.toll
               
                #Update node impedance
                new_imp = top_node.imp + imp
                # print('new imp time ' + str(new_imp) + ' b node imp ' + str(b_node.imp))
                # time.sleep(1)
                if new_imp < b_node.imp:
                    if b_node.imp == max_imp:
                        b_node.imp = new_imp
                        # print('insert b node ' + str(b_node.n))
                        # time.sleep(1)
                        my_heap.insert(b_node)                        
                    else:
                        b_node.imp = new_imp
                        # print('increase priority ' + str(b_node.n))
                        # time.sleep(1)
                        my_heap.increase_priority(b_node)
                        # print('lower imp for node ' + str(b_node.n))
                        # time.sleep(1)

                    b_node.time = top_node.time + link_time
                    b_node.dist = top_node.dist + nxlink.dist
                    b_node.toll = top_node.toll + nxlink.toll
                    b_node.parent = top_node
                    b_node.parent_link = nxlink
                    b_node.ts = curr_ts
                    
                    if b_node.ni == d_node_index:
                        return
    
    #trace path
    cpdef trace(self, sp_task):
        cdef int d_node_index, path_size
        cdef td.node *curr_node

        d_node_index = self.node_index[sp_task[1]]
        curr_node = &self.nodes[d_node_index]
        # load volume
        while curr_node.n != sp_task[0]:
            curr_node.parent_link.vol[curr_node.ts] += sp_task[4]
            curr_node = curr_node.parent


        d_node = &self.nodes[d_node_index]    
        # print('skim time ' + str(d_node.time) + ' dist ' + str(d_node.dist))
        return (curr_node.time, curr_node.dist)
    
    cpdef get_vol(self):
        cdef int i
        cdef td.link *link
        
        vol = np.zeros((self.num_links, self.num_time_steps))

        for i in range(self.num_links):
            for j in range(self.num_time_steps):
                vol[i][j]=self.links[i].vol[j]  
        
        return vol

 