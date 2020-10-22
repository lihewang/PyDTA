# -*- coding: utf-8 -*-
"""
Created on Thu Oct 15 11:15:09 2020

@author: lihe.wang
"""
import pyximport; pyximport.install()
import heap as hp
import pandas as pd
import sys

if __name__ == "__main__": 
      
    print('The priority queue heap example') 
    items = pd.Series([0.7,0.2,0.3,0.4,0.5,0.6,0.1,80,9],index=[7,0,3,40,5,6,1,8,9])
    #items = pd.Series([99,0.1,0.2,0.3],index=[0,1,2,3])
    my_heap = hp(100, items)  

    for i in items.items():
        my_heap.insert(i[0])
    #items[1] = 0.1
    #my_heap.change_priority(1)
    my_heap.print_heap()
    
    sq = "From low to high "
    for i in range(len(items)):
        min = my_heap.pop()
        sq += ", " + str(min) + "(" + str(items[min]) + ")"
    print(sq) 
    print("Number of heap element = " + str(my_heap.size+1))
    print("Heap memory size = " + str(sys.getsizeof(my_heap.minheap)) + " bytes")