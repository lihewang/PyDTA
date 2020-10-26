# -*- coding: utf-8 -*-
"""
Created on Thu Oct  1 09:06:42 2020

@author: lihe.wang
"""
import pyximport; pyximport.install()
import numpy as np
cimport numpy as np
cimport cython

cdef class heap:
    cdef public int size, FRONT
    cdef float[:] items
    cdef int[:] minheap, item_pos

    def __init__(self, int size, items):
        self.size = 0   #start at pos 1
        self.FRONT = 1  #start at pos 1
        self.minheap = np.zeros(size+1, dtype='i')
        self.minheap[0] = -1
        self.items = items
        self.item_pos = np.zeros(size+1, dtype='i')
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.cdivision(True)            
    cdef int parent(self, int pos):
        return pos//2
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.cdivision(True)
    cdef int left_child(self, int pos):
        return pos*2
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.cdivision(True)
    cdef int right_child(self, int pos):
        return (pos*2)+1
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    @cython.cdivision(True)
    cdef bint is_leaf(self, int pos): 
        if pos > (self.size//2) and pos <= self.size: 
            return True
        return False
    
    #called
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef is_empty(self):
        if self.size == 0:
            return True
        return False
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef swap(self, int fpos, int spos): 
        self.item_pos[self.minheap[fpos]] = spos
        self.item_pos[self.minheap[spos]] = fpos
        self.minheap[fpos], self.minheap[spos] = self.minheap[spos], self.minheap[fpos]
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef bubble_down(self, int pos): 
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
    
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef bubble_up(self, int pos):
        cdef int current = pos
        cdef int parent_node = self.minheap[self.parent(current)]
        if parent_node >= 0:
            while self.items[self.minheap[current]] < self.items[parent_node]: 
                self.swap(current, self.parent(current)) 
                current = self.parent(current) 
                parent_node = self.minheap[self.parent(current)]
                if parent_node < 0:
                    break
    
    #called
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef insert(self, int element):       
        self.size+= 1
        self.minheap[self.size] = element
        self.item_pos[element] = self.size
        self.bubble_up(self.size)
    
    #called
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef increase_priority(self, int element):
        self.bubble_up(self.item_pos[element])
    
    #called
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cpdef pop(self): 
        cdef int popped = self.minheap[self.FRONT] 
        self.item_pos[popped] = 0
        self.minheap[self.FRONT] = self.minheap[self.size] 
        self.minheap[self.size] = 0
        self.size-= 1
        if self.size > 0:
            self.bubble_down(self.FRONT) 
        return popped
    
    #for testing
    def bld_minheap(self): 
        for pos in range(self.size//2, 0, -1): 
            self.bubble_up(pos) 
            self.bubble_down(pos)
    
    #for testing
    def print_heap(self): 
        cdef int i
        for i in range(1, (self.size//2)+1): 
            print(" PARENT : " + str(self.minheap[i]) + "(" + str(self.items[self.minheap[i]]) + ")" +
                  " LEFT CHILD : " + str(self.minheap[2*i]) + "(" + str(self.items[self.minheap[2*i]]) + ")" +
                  " RIGHT CHILD : " + str(self.minheap[2*i+1])) 
                      


    