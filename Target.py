'''
Created on 2012-9-22

@author: pdl
'''

SIM = 0
ALL  = 3

from Lun import *

class Target():
    '''
    classdocs
    '''
    
    def __init__(self, str=None):

        if str == None:

            print 'fail to create target!'

            return

        strlist = str.split(':')

        for i in range(len(strlist)):

            self.index = strlist[0]

            self.name = (strlist[1] + ':' + strlist[2]).strip()

        self.lunlist = []
        

        
    def show(self, mode=ALL):
        
        print '<<<<<<<<<<', self.index, ':', self.name, '>>>>>>>>>>'
        
#        print 'lun num :',len(self.lunlist)
        if mode == ALL:
            
            for i in range(len(self.lunlist)):
               
                self.lunlist[i].show()
                      
            
    def add_lun(self, lun=None):
       
        if lun == None:
            
            print 'failed to add lun ',lun
            
            return False
            
        self.lunlist.append(lun)
                             
                             
    
    def delete_lun(self, lun_index=None):
        
        if lun == None:
          
            return False
       
        for lu in self.lunlist:
         
            if lun_index == filter(str.isdigit, lu.index):
                
                self.lunlist.remove(lu)
                              
                return True
            
        print 'failed to delete lun:',lun_index
        
        return False        
    
            
            
if __name__ == '__main__':
    
#    lun = Lun('LUN: null', '//', 0)
    
    tgt = Target('Target0: iqn.test:node0')
    
    tgt.show()
    
    lun = Lun('null', '//', 0)
    
    tgt.add_lun(lun)
    
    tgt.show()