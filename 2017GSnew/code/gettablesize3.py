#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import pprint
# import psycopg2
from psycopg2 import pool
import time
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool

dbname='hisdb'
connPool=""


def getConf(dbname='postgres'):
    from ConfigParser import ConfigParser
    conFile = r"./database.ini"
    conFig = ConfigParser()
    conFig.read(conFile)
    return (conFig.get(dbname, 'database'), conFig.get(dbname, 'user'), conFig.get(dbname, 'password'), conFig.get(dbname, 'host'), conFig.get(dbname, 'port'))

def initDbPool(dbname):
    (database, db_user, db_pwd, db_host, db_port) = getConf(dbname)
    dbpool = pool.ThreadedConnectionPool(minconn=45,
            maxconn=45,
            database=database,
            user=db_user,
            password=db_pwd,
            host=db_host,
            port=db_port
        )
    return dbpool

def writeDb(sql):
    """
    连接数据库（写），并进行写的操作，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回true
    """
    global connPool
    conn = getConn(connPool)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()  # 提交事务
        return True
    except Exception, e:
        conn.rollback()  # 如果出错，则事务回滚
        logging.error('数据写入失败:%s' % e)
        return False
    finally:
        cursor.close()
        connPool.putconn(conn)
    


def readDb(connPool,sql):
    """
    连接数据库（从），并进行数据查询，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回查询到的数据，这个数据是经过转换的，转成字典格式，方便模板调用，其中字典的key是数据表里的字段名
    """
    conn = getConn(connPool)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    # data = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]  # 转换数据，字典格式
    # cursor.close()
    # conn.commit()
    # connPool.putconn(conn)
    
        # 下面这句会报错，因为下面使用了生成器要求，不能有返回值，否则会报SyntaxError: 'return' with argument inside generator
        # return False
        # return
    
     
       
    for row in cursor.fetchall():
        temp={}
        for i,value in  enumerate(row):
            temp[cursor.description[i][0]]=value
                # yield dict(cursor.description[i][0],value)
        yield temp
    cursor.close()
    conn.commit()
    connPool.putconn(conn)
                
                

    
    # return data
    
        

def getConn(dbPool):
    import random
    try:
        conn = dbPool.getconn()
        # print conn
        return conn
    except Exception, e:
        print type(e),e
        logging.error('''数据库连接失败:%s''' % e)
        exit(-1)
def sumSize(tabData):
    sql="insert into dis.tc_table_space_tj_20170707_d(schemaname,relname,size) select '%s','%s',pg_relation_size('%s');" % (tabData['schemaname'],tabData['tablename'],tabData['schemaname']+'.'+tabData['tablename'])
    writeDb(sql)
    # print sql    
def main():
    '''
    主程序入口
    '''
    # 定义日志输出格式
    global connPool
    connPool=initDbPool(dbname)
    starttime = time.time()
    pool = ThreadPool(45) 
    sql = "select schemaname,tablename from pg_catalog.pg_tables b where b.schemaname <>'public' and b.schemaname <>'pg_catalog' and b.schemaname <> 'information_schema' and b.schemaname<>'gp_toolkit';"
    pool.map(sumSize, readDb(connPool,sql)) 
    pool.close() 
    pool.join()
    connPool.closeall()
    endtime = time.time() - starttime
    print endtime 
   
    
    # print getConf(db_name)
    # sql = "select schemaname,tablename from pg_catalog.pg_tables b where b.schemaname <>'public' and b.schemaname <>'pg_catalog' and b.schemaname <> 'information_schema' and b.schemaname<>'gp_toolkit';"
    # sql="select * from gp_biao_column_to_row limit 10;"
    # for dd in readDb(sql):
    #     print dd['schemaname'],dd['tablename']

    # print "现在开始计算表大小"
    # starttime = time.time()
    # # pool = ThreadPool(6) 
    # pool = Pool(1) 
    # pool.map(sumSize, readDb(sql)) 
    # pool.close() 
    # pool.join()
    # endtime = time.time() - starttime
    # print endtime 
    # print "现在开始计算表记录和楼"
    # starttime = time.time()
    # pool = ThreadPool(6) 
    # pool = Pool(20) 
    # pool.map(sumCount, readDb(sql)) 
    # pool.close() 
    # pool.join()
    # endtime = time.time() - starttime
    # print endtime 
    
    # for dd in data:
    #     print dd
    
    # data = ('huawei','huawei','huawei')
    # sql="insert into public.parse_result (proc_name,tar_name,src_name) values (%s,%s,%s);"
    # writeDb(conn,sql,data)
    # conn.close()

    # print data
    # for item in data:
    #     print item['typ']
    #     print "\n"
    # print getConf("hisdb")


if __name__ == '__main__':
    main()