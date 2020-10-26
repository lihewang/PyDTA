# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 15:26:34 2020

@author: lihe.wang

Test heap.pyx
"""

import heap as hp
import numpy as np
import sys
import timeit

if __name__ == "__main__":
    t1 = timeit.default_timer() 
    print('The priority queue heap example')
    imp = np.flip(np.arange(10000, dtype='f'))
    items = np.array(imp)
    #items = np.array([0.7,0.2,0.3,0.4,0.5,0.6,0.1,80,9], dtype='f')
    
    my_heap = hp.heap(50001, items) 
    
    for i in range(items.size):
        my_heap.insert(i)
        
    items -= 0.1  
    for i in range(items.size):        
        my_heap.increase_priority(i)
    #my_heap.print_heap()
    
    sq = "From low to high top 10"
    for i in range(10):
        min = my_heap.pop()
        sq += ", " + str(min) + "(" + str(items[min]) + ")"
    print(sq) 
    print("Number of heap element = " + str(my_heap.size+1))
    print("Heap memory size = " + str(sys.getsizeof(my_heap)) + " bytes")
    t2 = timeit.default_timer() 
    print(f"Run time is {t2 - t1:0.6f} seconds")