#!/usr/bin/env python
# -*- coding: utf-8 -*-
import utils
import time
import os,sys
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
import datetime

'''
多线程删除表，看能否快一点,20171204能快，但是发现有个别删除不了，而且不报错的
'''


def Dodelete(tabDatas):
    global dbconnPool
    # global today
    
   
    # from dateutil.relativedelta import relativedelta
    # global yesterday
    
    
    # sqls=["insert into dis.tc_table_space_tj_"+today+"_d(schemaname,relname,size) select '%s','%s',pg_relation_size('%s');" % (tabData['schemaname'],tabData['tablename'],tabData['schemaname']+'.'+tabData['tablename']) for tabData in tabDatas]
    sqls=["drop table %s CASCADE;" % (tabData) for tabData in tabDatas]
    print sqls
    utils.writeDbDDL(dbconnPool,sqls)
    

def DeleteTable(filename,bulkcount):
    # print "GetTableStatic"
    # global connPool
  
    starttime = time.time()
    utils.logging.info('delete job  start at %.2f' % starttime)
    pool = ThreadPool(Tcount) 
    # readdb后批量返回的条数
    
    pool.map(Dodelete, ReadfileBulkReturn(filename, bulkcount)) 
    # print ReadfileBulkReturn(filename, bulkcount)
    pool.close() 
    pool.join()
    dbconnPool.closeall()
    endtime = time.time() - starttime
    utils.logging.info('delete job   spend time is %.2f' % endtime)

def ReadfileBulkReturn(filename,bulkcount):
    data=[]
    for line in open(filename, "r"):
        data.append(line.strip())
    n=bulkcount
    for tempdata in [data[i:i+n] for i in range(0, len(data), n)]:
         yield tempdata



    
if __name__ == '__main__':
    
    Tcount = 5
    dbconnPool = utils.initDbPool("hisdb",minc=Tcount,maxc=Tcount)
    today = int(datetime.date.today().strftime("%Y%m%d"))
    bulkcount = 5
    dirname, _ = os.path.split(os.path.abspath(sys.argv[0]))
    delete_input =dirname + os.sep + "DropTable.txt"
    DeleteTable(delete_input,bulkcount)
    # dbconnPool.closeall()

    
        
    