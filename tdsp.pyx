# -*- coding: utf-8 -*-
"""
Created on Fri Sep 25 09:14:22 2020

@author: lihe.wang
"""

import numpy as np
cimport numpy as np
import pandas as pd
import heap as hp
import timeit

#Time Dependent Shortest Path
cdef class tdsp:
    cdef int num_nodes, i, path_size, curr_node, curr_ts_col
    cdef int o_node_index, d_node_index, start_ts, top_node_index, curr_ts, b_node
    cdef float new_imp, imp, t 
    cdef float[:] node_imp, sp_task, args
    #cdef float[:,:] link_attribute
    cdef int[:] popped, path 
    cdef object my_heap, nxlinks, link_index, link_attribute, next_links, parent_node, path_skim, path_nodes, node_time, node_dist, node_toll
    
    def __init__(self, int num_nodes, next_links, link_attribute, args):
        #Node impedance and index
        self.num_nodes = num_nodes
        self.node_imp = np.array(np.full(num_nodes,args[1]), dtype='f') 
        self.node_time = np.zeros(self.num_nodes, dtype='f')
        self.node_dist = np.zeros(self.num_nodes, dtype='f')
        self.node_toll = np.zeros(self.num_nodes, dtype='f')
        self.my_heap = hp.heap(self.num_nodes, self.node_imp)
        self.next_links = next_links
        self.link_attribute = link_attribute
        self.args = args
        
    cpdef build(self, sp_task):       
        self.o_node_index = sp_task[0]
        self.d_node_index = sp_task[1]        
        self.start_ts = sp_task[2]
        self.node_imp[self.o_node_index] = 0
        self.my_heap.insert(self.o_node_index)
        self.parent_node = np.array(np.full(self.num_nodes,-1), dtype='i')
        self.popped = np.zeros(self.num_nodes, dtype='i')
        
        while not self.my_heap.is_empty():
            self.top_node_index = self.my_heap.pop()
            #print('pop node ' + str(self.top_node_index))
            self.popped[self.top_node_index] = 1     
            #Found destination
            if self.top_node_index == self.d_node_index:
                break
            
            #Calculate time step
            self.curr_ts = int(self.node_time[self.top_node_index]//15) + self.start_ts
            if self.curr_ts > 96:
                self.curr_ts -= 96
            self.curr_ts_col = self.curr_ts + 3
            
            #Next links index           
            self.nxlinks = self.next_links[self.top_node_index]
            for self.i in self.nxlinks[0]:
                self.b_node = int(self.link_attribute[self.i, 1])
                self.t = self.link_attribute[self.i, self.curr_ts_col]
                self.imp = self.t + self.args[2] * self.link_attribute[self.i, 3]
                #Path can't be backward       
                if self.b_node == self.top_node_index or self.popped[self.b_node] == 1:
                    continue
                #print('B node ' + str(self.b_node))
                #Update node impedance
                self.new_imp = self.node_imp[self.top_node_index] + self.imp
                if self.new_imp < self.node_imp[self.b_node]:
                    if self.node_imp[self.b_node] == self.args[1]:
                        self.my_heap.insert(self.b_node)
                        #print('insert node ' + str(self.b_node))
                    else:
                        self.my_heap.increase_priority(self.b_node)
                        #print('heapify node ' + str(self.b_node))
                    self.node_imp[self.b_node] = self.new_imp
                    self.node_time[self.b_node] = self.node_time[self.top_node_index] + self.t
                    self.node_dist[self.b_node] = self.node_dist[self.top_node_index] + self.link_attribute[self.i, 2]
                    self.node_toll[self.b_node] = self.node_toll[self.top_node_index] + self.link_attribute[self.i, 3]
                    self.parent_node[self.b_node] = self.top_node_index
                    
                #zones can't be transpassed    
                if self.top_node_index <= self.args[0] and self.top_node_index != self.o_node_index:
                    continue
        
        return self.parent_node 

    cpdef trace(self, sp_task, parent_node):
        self.path = np.zeros(self.num_nodes, dtype='i')
        self.path_size = 0
        self.d_node_index = sp_task[1]
        self.curr_node = self.d_node_index
        while self.curr_node != -1:
            self.path_size += 1
            self.path[self.path_size-1] = self.curr_node
            self.curr_node = parent_node[self.curr_node]
        
        self.path_nodes = np.flip(self.path[:self.path_size])
        self.path_skim = pd.DataFrame({'time':self.node_time[self.path_nodes], 
                                       'dist':self.node_dist[self.path_nodes], 
                                       'toll':self.node_toll[self.path_nodes]}, 
                                       index=self.path_nodes)
        return self.path_skim



