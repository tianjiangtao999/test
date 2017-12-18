#!/usr/bin/env python
# -*- coding: utf-8 -*-
import utils
import time
import os,sys

import datetime




'''
用来从Gp同步表名和一些其它内容到配置库。同步内容包含表名+用户名，表统计信息，
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
    


    
    
    
  

if __name__ == '__main__':
    # if len(sys.argv) < 2:
    #     print "the argv is lack!!! will exit！！！"
    #     exit(0)
    # else:
    Tcount = 1
    bulkcount = 1
    today=datetime.date.today().strftime("%Y%m%d")
    utils.logging.info('the daily sync from hisdb to configdb  start>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    dbconnPoolsrc = utils.initDbPool("hisdb",minc=Tcount,maxc=Tcount)
    dbconnPooldest = utils.initDbPool("configdb",minc=Tcount,maxc=Tcount)
    #清理configdb中的表
    utils.logging.info('before the  sync tbalename ，clear  table >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    GetTableNameSql = ""
    PutTableNamesql = "truncate table temp.tablename"
    strFormat=""
    utils.syncG2p(dbconnPoolsrc,dbconnPooldest,GetTableNameSql,PutTableNamesql,strFormat)
    #同步表名及owner
    utils.logging.info(' sync table name from hisdb to configdb running>>>>>>>>>>>>>>>>>')
    GetTableNameSql = "select a.schemaname,a.tablename,a.tableowner,to_char(b.statime,'yyyymmdd') tablecreattime  \
    from pg_tables a left join ( select * from pg_stat_operations where actionname ='CREATE'  \
    and objname !~'.*_1_prt.*') b on a.schemaname=b.schemaname  and a.tablename=b.objname   \
    where a.tablename !~'.*_1_prt.*' and a.tablename !~'.*dataflow.*' and a.schemaname<>'public'   \
    and a.schemaname<>'etl' and a.schemaname !~'.*test.*' and a.schemaname<>'information_schema'  \
    and a.schemaname<>'pg_catalog' and a.schemaname<>'gp_toolkit';"
    PutTableNamesql = "insert into temp.tablename(schema_name,table_name,owner,creattime) values ('%s','%s','%s','%s');"
    FormatStr=['schemaname','tablename','tableowner','tablecreattime']
    utils.syncG2p(dbconnPoolsrc,dbconnPooldest,GetTableNameSql,PutTableNamesql,FormatStr)

     #写temp.table_complete_log表记录
    sqls=["insert into temp.table_complete_log (schema_name,table_name,deal_date) values ('temp','tablename',"+today+");"]
    utils.writeDb(dbconnPooldest,sqls)
    
    utils.logging.info(' sync table name from hisdb to configdb ended>>>>>>>>>>>>>>>>>>>')
    #清理configdb中的表统计信息表，否则跑两次的话会导致重复
    
    utils.logging.info('before sync the table statics info，clear  table >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    GetTableNameSql = ""
    PutTableNamesql = "truncate table temp.tablestaticinfo"+today+";"
    strFormat=""
    utils.syncG2p(dbconnPoolsrc,dbconnPooldest,GetTableNameSql,PutTableNamesql,strFormat)
    "同步表的统计信息"
    
    utils.logging.info('the daily sync table static from hisdb to configdb starting>>>>>>>>>>>>>>') 
    #原来在today的位置是max(deal_date)但是会出现删除的表还在这个里面，所以改成today，但是会有一些表统计失败的。
    # 但是这样不行，因为昨天在175上难以取得，只能改回，统一删除表,所以想了每天取数之前先清理一下数据，把已经删除的信息从统计表中删除
    #清理已经删除的表，在gettablesize中
  

    GetTableNameSql = "select schemaname,tablename,round(ccc*1.0/1024/1024,0) tablesize,tablerows, partitions from \
                       (select a.schemaname,case when relname like '%_1_prt%' then substr(relname,1,position('_1_prt' in relname)-1)  \
                       else relname end tablename,sum(size) ccc,sum(rownum) tablerows,count(1) partitions from etl.tc_table_space_tj_d  \
                       a join (select partitionschemaname,partitiontablename from pg_partitions ) m on  \
                       a.schemaname=m.partitionschemaname and a.relname=m.partitiontablename group by  \
                       a.schemaname,case when relname like '%_1_prt%' then substr(relname,1,position('_1_prt' in relname)-1) \
                        else relname end union all select schemaname,relname,size ccc,rownum tablerows,0 from  \
                        etl.tc_table_space_tj_d where  (schemaname,relname,deal_date) in  \
                        (select schemaname,relname,max(deal_date) cc from etl.tc_table_space_tj_d group by schemaname,relname)  \
                        and relname not like '%_1_prt_%') p order by tablesize desc"
    PutTableNamesql = "insert into temp.tablestaticinfo"+today+" (SCHEMA_NAME,TABLE_NAME,TABLE_SIZE,TABLE_ROWNUM,PARTITION_NUMS,deal_date) values ('%s','%s','%s','%s','%s',"+today+");"
    
    FormatStr=['schemaname','tablename','tablesize','tablerows','partitions']
    utils.syncG2p(dbconnPoolsrc,dbconnPooldest,GetTableNameSql,PutTableNamesql,FormatStr)

    #写temp.table_complete_log表记录
    sqls=["insert into temp.table_complete_log (schema_name,table_name,deal_date) values ('temp','tablestaticinfo"+today+"',"+today+");"]
    utils.writeDb(dbconnPooldest,sqls)
    utils.logging.info('the daily sync table static from hisdb to configdb  ended>>>>>>>>>>>>>>>>>>>')
   #清理configdb中的表统计信息表，否则跑两次的话会导致重复
    
    utils.logging.info('before sync the data.tb_bl_boss ，clear  table >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
    GetTableNameSql = ""
    PutTableNamesql = "truncate table data.tb_bl_boss;"
    strFormat=""
    utils.syncG2p(dbconnPoolsrc,dbconnPooldest,GetTableNameSql,PutTableNamesql,strFormat)


    "同步表etl.tb_bl_boss的信息"
    
    utils.logging.info('the daily sync etl.tb_bl_boss from hisdb to configdb starting>>>>>>>>>>>>>>') 
    #原来在today的位置是max(deal_date)但是会出现删除的表还在这个里面，所以改成today，但是会有一些表统计失败的。
    GetTableNameSql = "select cro_no,unit_type,unit_no, db_name,t_owner,t_name,t_marks,col_name,order_id,col_len,col_marks,last_time ,deal_date from etl.tb_bl_boss;"
    PutTableNamesql = "insert into data.tb_bl_boss (cro_no,unit_type,unit_no,db_name,t_owner,t_name,t_marks,col_name,order_id,col_len,col_marks,last_time,deal_date) values ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
    
    FormatStr=['cro_no','unit_type','unit_no','db_name','t_owner','t_name','t_marks','col_name','order_id','col_len','col_marks','last_time','deal_date']
    utils.syncG2p(dbconnPoolsrc,dbconnPooldest,GetTableNameSql,PutTableNamesql,FormatStr)
    utils.logging.info('the daily sync etl.tb_bl_boss from hisdb to configdb  ended>>>>>>>>>>>>>>>>>>>')
    
    #写temp.table_complete_log表记录
    sqls=["insert into temp.table_complete_log (schema_name,table_name,deal_date) values ('data','tb_bl_boss',"+today+");"]
    utils.writeDb(dbconnPooldest,sqls)

    dbconnPoolsrc.closeall()
    dbconnPooldest.closeall()
