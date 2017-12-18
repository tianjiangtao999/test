#!/usr/bin/env python
# -*- coding: utf-8 -*-
import utils
import time
import os,sys
from dateutil.relativedelta import relativedelta
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool

def DropTable(tablenames):
    global dbconnPool 
    
    sqls=["drop table '%s';" % (tablename['tablename']) for tablename in tablenames]
    # print sqls
    utils.writeDb(dbconnPool,sqls)
    
if __name__ == '__main__':
    Tcount = 10
    bulkcount = 100
    dbconnPool = utils.initDbPool("hisdb",minc=Tcount,maxc=Tcount)
    premonth = (datetime.date.today()+relativedelta(months=-2)).strftime("%Y%m%d %H:%M:%S")
    
    sql = "select distinct(schemaname||'.'||objname) tablename from pg_stat_operations a where schemaname ~'.*test.*' and a.statime < '"+premonth+"' and a.actionname ='CREATE' and objname !~'.*_1_prt.*';"
    print sql
    
    starttime = time.time()
    utils.logging.info('the monthly job drop test schema start at %.2f' % starttime)
    pool = ThreadPool(Tcount) 
    # readdb后批量返回的条数
    pool.map(DropTable, utils.ReadDbBulkReturn(dbconnPool,sql,bulkcount)) 
    pool.close() 
    pool.join()
    dbconnPool.closeall()
    endtime = time.time() - starttime
    utils.logging.info('the monthly job drop test schema spend time is %.2f' % endtime)
 