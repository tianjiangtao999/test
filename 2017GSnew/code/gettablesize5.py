#!/usr/bin/env python
# -*- coding: utf-8 -*-
import utils
import time
import os,sys
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
import datetime

'''
在gettablesize4.py的基础上修改，尝试使用utils外部表来封装公共函数。GetTableStatic是每天晚上运行的查找表统计信息的表，
因为发现其中总有一些统计失败的条目，但是数量不多，所以第二天修正一下，GetTableStaticCorrect。
'''


def sumSize(tabDatas):
    global dbconnPool
    global today
    
   
    # from dateutil.relativedelta import relativedelta
    # global yesterday
    
    
    # sqls=["insert into dis.tc_table_space_tj_"+today+"_d(schemaname,relname,size) select '%s','%s',pg_relation_size('%s');" % (tabData['schemaname'],tabData['tablename'],tabData['schemaname']+'.'+tabData['tablename']) for tabData in tabDatas]
    sqls=["insert into etl.tc_table_space_tj_d(schemaname,relname,size,rownum,deal_date) select '%s','%s', pg_relation_size('%s'),(select count(1)  from %s),'%s';" % (tabData['schemaname'],tabData['tablename'],tabData['schemaname']+'.'+tabData['tablename'],tabData['schemaname']+'.'+tabData['tablename'],today) for tabData in tabDatas]
    # print sqls
    utils.writeDb(dbconnPool,sqls)
    

def GetTableStatic(sql,bulkcount):
    # print "GetTableStatic"
    # global connPool
  
    starttime = time.time()
    utils.logging.info('the daily job  start at %.2f' % starttime)
    pool = ThreadPool(Tcount) 
    # readdb后批量返回的条数
    
    pool.map(sumSize, utils.ReadDbBulkReturn(dbconnPool,sql,bulkcount)) 
    pool.close() 
    pool.join()
    # dbconnPool.closeall()
    endtime = time.time() - starttime
    utils.logging.info('the daily job  spend time is %.2f' % endtime)



    
if __name__ == '__main__':
    
    Tcount = 30
    dbconnPool = utils.initDbPool("hisdb",minc=Tcount,maxc=Tcount)
    today = int(datetime.date.today().strftime("%Y%m%d"))
    bulkcount = 60
    sql = "select schemaname,tablename from pg_tables where tablename  \
        !~'.*_1_prt.*' and tablename !~'DW.DATAFLOW.*' and schemaname<>'public'  \
         and schemaname<>'etl' and schemaname !~'.*test.*' and  \
         schemaname<>'session' and schemaname<>'information_schema'  \
         and schemaname<>'pg_catalog' and schemaname<>'gp_toolkit'  \
         and schemaname||'.'||tablename not in (select distinct  \
         schemaname||'.'||tablename from pg_partitions) union all \
          select partitionschemaname schemaname,partitiontablename  \
          relname from  pg_partitions except select schemaname,relname  \
          from etl.tc_table_space_tj_d where relname ~'.*_1_prt.*';"
    utils.logging.info('the daily job to get the table size start >>>>>>>>>>>>>>>>>>> ')
    GetTableStatic(sql,bulkcount)
    # 从统计表中清除一下被删除的表
    sqls=["delete from etl.tc_table_space_tj_d where (schemaname,relname) in (select b.schemaname,\
    b.relname from (select schemaname,relname from etl.tc_table_space_tj_d  group by  schemaname,relname\
     ) b left join (select schemaname,tablename,tableowner from pg_tables) a on  a.schemaname=b.schemaname\
      and a.tablename=b.relname where a.schemaname is null);"]
    # print sqls
    utils.writeDb(dbconnPool,sqls)
    dbconnPool.closeall()

    
        
    