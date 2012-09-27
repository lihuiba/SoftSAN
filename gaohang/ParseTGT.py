'''
Created on 2012-9-23

@author: pdl
'''
import os,sys,string

nums = string.digits

from Target import *


 



def ParseLine(linelist):
    
    tgtlist = []
 
    for line in linelist:
       
        if line.find('Target') != -1:
            
            tgt = Target(line)
            
        elif line.find('LUN:') != -1:
            
            lun = Lun()
            
            lun.set_index(line)
          
        elif line.find('Size:') != -1:
            
            lun.set_size(line)
           
        elif line.find('path:') != -1:
            
            lun.set_path(line)
          
        elif line.find('flags:') != -1:
            
            tgt.add_lun(lun)
            
        elif line.find('ACL information:') != -1:
            
            tgtlist.append(tgt)
            
      
    return tgtlist
         
               
def help():
    
    print ''
    print '* * * * * * * * * * * * * * * * * * * * mytgt help information * * * * * * * * * * * * * * * * * * * *'
    print ''
    print 'your input should be as follow:'
    print ''
    print 'mytgt.py show MM'
    print ''
    print 'mytgt.py delete XX YY'
    print ''
    print 'mytgt.py new XX YY PATH'
    print ''
    print 'mytgt.py sub XX YY PATH'
    print ''
    print 'mytgt.py bind XX ADDR'
    print ''
    print 'mytgt.py unbind XX ADDR'
    print ''
#    print 'mytgt.py account UU PP'
#    print ''
#    print 'mytgt.pu account UU'
    
#    Create a new target device
    # tgtadm --lld iscsi --mode target --op new --tid=1 --targetname iqn.2009-02.com.example:for.all
#    Add a logical unit (LUN)
    # tgtadm --lld iscsi --mode logicalunit --op new --tid 1 --lun 1 -b /tmp/iscsi-disk1
    
    
#    IP-based restrictions
    # tgtadm --lld iscsi --mode target --op bind --tid 1 -I ALL
    # tgtadm --lld iscsi --mode target --op unbind --tid 1 -I ALL
    # tgtadm --lld iscsi --mode target --op bind --tid 1 -I 10.10.0.24
    # tgtadm --lld iscsi --mode target --op bind --tid 1 -I 10.10.0.0/24
    
    
#    CHAP Initiator Authentication
    # tgtadm --lld iscsi --mode account --op new --user ''consumer'' --password ''Longsw0rd''
    # tgtadm --lld iscsi --mode account --op bind --tid 1 --user ''consumer''
    
    
#    CHAP Target Authentication
    # tgtadm --lld iscsi --mode account --op new --user ''provider'' --password ''Shortsw0rd''
    # tgtadm --lld iscsi --mode account --op bind --tid 1 --user ''provider'' --outgoing

    
    print ''
    print 'XX: a number represents Target Index'
    print 'YY: a number represents Lun Index'
    print 'PATH: a absolute path '
    print 'ADDR: the ip address of initiator'
    print 'MM(mode): show all(default: ALL) or show simple(SIM)'
    print ''
    
    
    print '* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *'

def cmd_delete_tgt(tid):
    
    if tid.isdigit() != True:
       
       return None
    
    cmd = 'tgtadm --lld iscsi --mode target --op delete --tid '
    
    cmd += tid
    
    return cmd


def cmd_new_tgt(tid, tname):
    
    if tid.isdigit() != True:
        
        return None
    
    cmd = 'tgtadm --lld iscsi --mode target --op new --tid '+ tid +' -T '+tname
    
    return cmd


def cmd_delete_tgt(tid):
    
    if tid.isdigit() != True:
        
        return None
    
    cmd = 'tgtadm --lld iscsi --mode target --op delete --tid '
    
    cmd += tid
    
    return cmd



def cmd_new_lun(tid, lun_id, path):
    
    cmd = 'tgtadm --lld iscsi --mode logicalunit --op new --tid '+tid+' --lun '+lun_id+' -b '+path
    
    return cmd


def cmd_delete_lun(tid, lun_id):
    
    cmd = 'tgtadm --lld iscsi --mode logicalunit --op delete --tid '+ tid + ' --lun '+ lun_id
   
    return cmd


def cmd_bind_tgt(tid, ip='ALL'):
    
    cmd = 'tgtadm --lld iscsi --mode target --tid '+ tid +' --op bind -I '+ ip
    
    return cmd


def cmd_unbind_tgt(tid, ip='ALL'):

    cmd = 'tgtadm --lld iscsi --mode target --tid '+ tid + '--op unbind --I ' + ip

    return cmd


def cmd_new_account(usr, pswd):
    
    cmd = 'tgtadm --lld iscsi --mode account --op new --user '+ usr +' --password '+ pswd
    
    return cmd


def cmd_delete_account(usr, pswd):
    
    cmd = 'tgtadm --lld iscsi --mode account --op delete --user '+ usr
    
    return cmd



def cmd_bind_account(tid, usr):
    
    cmd = 'tgadm --lld iscsi --mode account --op bind --tid '+ tid+' --user '+usr
    
    return cmd


def cmd_unbind_account(tid, usr):
    
    cmd = 'tgadm --lld iscsi --mode account --op unbind --tid '+ tid+' --user '+usr
    
    return cmd



def main():
    
    argv = sys.argv
    
    argc = len(argv)
    
    if argc<2 or argc>7 or argv[1] == 'help':
        
        help()
        
        return 
    
    cmd = 'tgtadm --lld iscsi --mode target --op show '
    
    linelist = os.popen(cmd)
    
    tgtlist = ParseLine(linelist)
    
    if argv[1] == 'show':
        
        if argc == 2:
                        
            os.system(cmd)
                
        elif argc == 3:
            
            if argv[2] == 'SIM':
                
                for tgt in tgtlist:
                
                    tgt.show('SIM')
                    
            else:
                
                for tgt in tgtlist:
                
                    tgt.show()
                    
        else:
            
            print 'Invalid Input !'
            
            help()
                
    elif argv[1] == 'new':
        
        tid = argv[2]
        
        lun_id = argv[3]
        
        path = argv[4]
        
        cmd = cmd_new_lun(tid, lun_id, path)
        
        print 'your cmd>>>>>>>>>>',cmd
        
        os.system(cmd)
        
    elif argv[1] == 'delete':
        
        tid = argv[2]
        
        lun_id = argv[3]
        
        cmd = cmd_delete_lun(tid, lun_id)
        
        print 'your cmd>>>>>>>>>>',cmd
        
        os.system(cmd)
    
    elif argv[1] == 'sub':
        
        tid = argv[2]
        
        lun_id = argv[3]
        
        path = argv[4]
        
        cmd = cmd_delete_lun(tid, lun_id)
        
        os.system(cmd)
        
        cmd = cmd_new_lun(tid, lun_id, path)
        
        os.system(cmd)
        
    elif argv[1] == 'bind':
        
        tid = argv[2]
        
        ip = argv[3]
        
        cmd = cmd_bind_tgt(tid, ip)
        
        os.system(cmd)
        
    else:
        
        print 'Invalid input!'
        
        help()
        
    return
        
    
            
    
    


if __name__ == '__main__':
    
    main()
    
    
        
    
        
    
    
    
