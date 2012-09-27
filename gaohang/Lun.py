'''
Created on 2012-9-23

@author: pdl
'''
INDEX = 0
SIZE = 1
PATH = 2
ALL = 3

class Lun(object):
    '''
    classdocs
    '''
    


    def __init__(self, index='', path='', size='Size: 0'):
        '''
        Constructor
        '''
        self.index = index
        
        self.path = path
        
        self.size = size
        
        
    def set_index(self, textline):
        
        if textline == None:
            
            return False
        
        self.index = textline.strip()     
         
        return True
    
    
    def set_path(self, textline):
      
        if textline == None:
            
            return False        
        
        self.path = textline
        
        return True        
                
                
    def set_size(self, textline):
        
        if textline == None:
            
            return False     
        
        self.size = textline
        
        return True       
     
        
    def show(self, mode=ALL):
        
        if mode==ALL:
            
            print '-----', self.index, '-----'
            
            print self.path.strip()
            
            print self.size.strip()
            
            print ''
            
        elif mode==INDEX:
            
            print '-----', self.index, '-----' 
            
        
        
if __name__ == '__main__':
    
    lun = Lun('null', '//', 0)
    
    lun.show()
    