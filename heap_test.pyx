# -*- coding: utf-8 -*-
#cython: language_level=3
"""
Created on Fri Oct 16 15:26:34 2020

@author: lihe.wang

Test heap.pyx
"""
from cymem.cymem cimport Pool
cimport typedef as td
cimport heap as hp
import timeit

cpdef heap_test():
    cdef int num_nd = 10000, i
    cdef Pool mem = Pool()
    cdef td.node* nds = <td.node*>mem.alloc(num_nd, sizeof(td.node))
    cdef td.node *min_nd
    cdef hp.heap my_heap
    
    t1 = timeit.default_timer()
    print('The priority queue heap example') 
    my_heap = hp.heap(num_nd)     
    for i in range(10000):
        nds[i].n = i
        nds[i].imp = num_nd - i
        my_heap.insert(&nds[i])
        
    nds[999].imp = 0.1
    my_heap.increase_priority(&nds[999])
    
    for i in range(10):
        min_nd = my_heap.pop()
        print('min node n=' + str(min_nd.n) + ' imp=' + str(min_nd.imp))
    t2 = timeit.default_timer() 
    print(f"Run time is {t2 - t1:0.6f} seconds")
    
