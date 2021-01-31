# -*- coding: utf-8 -*-
#cython: language_level=3
"""
Created on Jan 14 2021
priority heap for Dijkstra algorithm
parent: pos//2
left_child: pos*2
right_childe pos*2+1
@author: Lihe Wang
"""

from cymem.cymem cimport Pool

cdef class heap:
    
    def __cinit__(self, int num_nd):
        self.size = 0   #start at pos 1
        self.FRONT = 1  #start at pos 1
        self.mem = Pool()
        self.minheap = <td.node**>self.mem.alloc(num_nd, sizeof(td.node*))
        self.minheap[0] = NULL
        self.ndpos = array.array('I', [0]*num_nd)
        
    cdef swap(self, int fpos, int spos): 
        self.ndpos[self.minheap[fpos].ni] = spos
        self.ndpos[self.minheap[spos].ni] = fpos
        self.minheap[fpos], self.minheap[spos] = self.minheap[spos], self.minheap[fpos]
    
    cdef bubble_down(self, int pos): 
        # If the node is a non-leaf node and greater than any of its child 
        if not (pos > (self.size//2) and pos <= self.size):
            #If the node has right child
            if (pos*2)+1 <= self.size:
                if (self.minheap[pos].imp > self.minheap[pos*2].imp or 
                   self.minheap[pos].imp > self.minheap[(pos*2)+1].imp): 
      
                    # Swap with the left child and heapify the left child 
                    if self.minheap[pos*2].imp < self.minheap[(pos*2)+1].imp: 
                        self.swap(pos, pos*2) 
                        self.bubble_down(pos*2) 
      
                    # Swap with the right child and heapify the right child 
                    else: 
                        self.swap(pos, (pos*2)+1) 
                        self.bubble_down((pos*2)+1) 
            else:
                if self.minheap[pos].imp > self.minheap[pos*2].imp:
                    self.swap(pos, pos*2) 
                    self.bubble_down(pos*2)
    

    cdef bubble_up(self, int pos):
        cdef int current = pos
        cdef td.node* parent_node = self.minheap[current//2]
        if parent_node != NULL:
            while self.minheap[current].imp < parent_node.imp: 
                self.swap(current, current//2) 
                current = current//2
                parent_node = self.minheap[current//2]
                if parent_node == NULL:
                    break

    cdef insert(self, td.node *nd): 
        self.size+= 1
        self.minheap[self.size] = nd
        self.ndpos[nd.ni] = self.size
        #print('node ' + str(nd.n) + ' imp ' + str(nd.imp) + ' insert at ' + str(self.size))
        self.bubble_up(self.size)
      

    cdef td.node* pop(self): 
        cdef td.node *popped = self.minheap[self.FRONT] 
        self.ndpos[popped.ni] = 0
        self.minheap[self.FRONT] = self.minheap[self.size] 
        self.minheap[self.size] = NULL
        self.size-= 1
        if self.size > 0:
            self.bubble_down(self.FRONT) 
        return popped
    
    cdef increase_priority(self, td.node *nd):
        self.bubble_up(self.ndpos[nd.ni])
        
    cdef bint is_empty(self):
        if self.size == 0:
            return True
        return False
    



    