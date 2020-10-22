# -*- coding: utf-8 -*-
"""
Created on Thu Oct  1 09:06:42 2020

@author: lihe.wang
"""
import numpy as np
import pandas as pd
import sys

class heap:

    def __init__(self, size, items):
        self.size = 0   #start at pos 1
        self.FRONT = 1  #start at pos 1
        self.minheap = np.zeros(size+1, dtype=np.int16) #item index
        self.minheap[0] = -1
        self.items = items
        self.item_pos = np.zeros(size+1, dtype=np.int16)
                
    def parent(self, pos):
        return pos//2
    
    def left_child(self, pos):
        return pos*2
    
    def right_child(self, pos):
        return (pos*2)+1
    
    def is_leaf(self, pos): 
        if pos > (self.size//2) and pos <= self.size: 
            return True
        return False
    
    def is_empty(self):
        if self.size == 0:
            return True
        return False
    
    def swap(self, fpos, spos): 
        self.item_pos[self.minheap[fpos]] = spos
        self.item_pos[self.minheap[spos]] = fpos
        self.minheap[fpos], self.minheap[spos] = self.minheap[spos], self.minheap[fpos]
    

    def bubble_down(self, pos): 
        # If the node is a non-leaf node and greater than any of its child 
        if not self.is_leaf(pos):
            #If the node has right child
            if self.right_child(pos) <= self.size:
                if (self.items[self.minheap[pos]] > self.items[self.minheap[self.left_child(pos)]] or 
                   self.items[self.minheap[pos]] > self.items[self.minheap[self.right_child(pos)]]): 
      
                    # Swap with the left child and heapify the left child 
                    if self.items[self.minheap[self.left_child(pos)]] < self.items[self.minheap[self.right_child(pos)]]: 
                        self.swap(pos, self.left_child(pos)) 
                        self.bubble_down(self.left_child(pos)) 
      
                    # Swap with the right child and heapify the right child 
                    else: 
                        self.swap(pos, self.right_child(pos)) 
                        self.bubble_down(self.right_child(pos)) 
            else:
                if self.items[self.minheap[pos]] > self.items[self.minheap[self.left_child(pos)]]:
                    self.swap(pos, self.left_child(pos)) 
                    self.bubble_down(self.left_child(pos))
    
    def bubble_up(self, pos):
        current = pos
        parent_node = self.minheap[self.parent(current)]
        if parent_node >= 0:
            while self.items[self.minheap[current]] < self.items[parent_node]: 
                self.swap(current, self.parent(current)) 
                current = self.parent(current) 
                parent_node = self.minheap[self.parent(current)]
                if parent_node < 0:
                    break
        
    def insert(self, element):    #element is index   
        self.size+= 1
        self.minheap[self.size] = element
        self.item_pos[element] = self.size
        self.bubble_up(self.size)
            
    def increase_priority(self, element):
        self.bubble_up(self.item_pos[element])
                
    def pop(self): 
        popped = self.minheap[self.FRONT] 
        self.item_pos[popped] = 0
        self.minheap[self.FRONT] = self.minheap[self.size] 
        self.minheap[self.size] = 0
        self.size-= 1
        if self.size > 0:
            self.bubble_down(self.FRONT) 
        return popped
    
    def bld_minheap(self): 
        for pos in range(self.size//2, 0, -1):
            self.bubble_up(pos)
            self.bubble_down(pos) 
        
    def print_heap(self): 
        for i in range(1, (self.size//2)+1): 
            print(" PARENT : " + str(self.minheap[i]) + "(" + str(self.items[self.minheap[i]]) + ")" +
                  " LEFT CHILD : " + str(self.minheap[2*i]) + "(" + str(self.items[self.minheap[2*i]]) + ")" +
                  " RIGHT CHILD : " + str(self.minheap[2*i+1])) 

#Test code    
if __name__ == "__main__": 
      
    print('The priority queue heap example') 
    items = np.array([0.6,0.2,0.3,0.4], dtype='f')
    my_heap = heap(items.size, items)  

    for i in range(items.size):
        my_heap.insert(i)
    items[3] = 0.05
    my_heap.increase_priority(3)
    #my_heap.print_heap()
    
    sq = "From low to high "
    for i in range(items.size):
        min = my_heap.pop()
        sq += ", " + str(min) + "(" + str(items[min]) + ")"
    print(sq) 
    print("Number of heap element = " + str(my_heap.size+1))
    print("Heap memory size = " + str(sys.getsizeof(my_heap.minheap)) + " bytes")
    