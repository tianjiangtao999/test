#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import pprint
import psycopg2

error_log =r"./tt.log"
logging.basicConfig(level=logging.ERROR,format = '%(asctime)s %(filename)s[line:%lineno)d] %(levelname)s %(message)s',datefmt = '%Y-%m-%d %H:%M:%S',filename = error_log,filemode = 'a')

def writeDb(conn,sql, data):
    """
    连接数据库（写），并进行写的操作，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回true
    """
    cursor = conn.cursor()
    try:
        cursor.execute(sql, data)
        conn.commit()  # 提交事务
    except Exception, e:
        conn.rollback()  # 如果出错，则事务回滚
        logging.error('数据写入失败:%s' % e)
        return False
    finally:
        cursor.close()
        
    return True


def readDb(conn,sql):
    """
    连接数据库（从），并进行数据查询，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回查询到的数据，这个数据是经过转换的，转成字典格式，方便模板调用，其中字典的key是数据表里的字段名
    """
    
    try:
        cursor = conn.cursor()
    except Exception, e:
        print e
        logging.error('数据库连接失败:%s' % e)
        # 下面这句会报错，因为下面使用了生成器要求，不能有返回值，否则会报SyntaxError: 'return' with argument inside generator
        # return False
        # return
    try:
        cursor.execute(sql)
        for row in cursor.fetchall():
            for i,value in  enumerate(row):
                # yield dict(cursor.description[i][0],value)
                yield dict([(cursor.description[i][0],value)])
                
                

        # data = [dict((cursor.description[i][0], value) for i, value in enumerate(
        #     row)) for row in cursor.fetchall()]  # 转换数据，字典格式
    except Exception, e:
        print e
        pass
        # logging.error('数据执行失败:%s' % e)
        # return 
    finally:
        cursor.close()
        


def getConf(dbname='postgres'):
    from ConfigParser import ConfigParser
    conFile = r"./database.ini"
    conFig = ConfigParser()
    conFig.read(conFile)
    return (conFig.get(dbname, 'database'), conFig.get(dbname, 'user'), conFig.get(dbname, 'password'), conFig.get(dbname, 'host'), conFig.get(dbname, 'port'))


def getConn(dbname='postgres'):
    
    (database, db_user, db_pwd, db_host, db_port) = getConf(dbname)
    try:
        conn = psycopg2.connect(database=database, user=db_user, password=db_pwd, host=db_host, port=db_port)
        return conn
    except Exception, e:
        print type(e),e
        logging.error('''数据库连接失败:%s''' % e)
        exit(-1)
    


def main():
    '''
    主程序入口
    '''
    

    # 定义日志输出格式

   
    db_name = "hisdb"
    # print getConf(db_name)
    sql = "select schemaname,tablename from pg_catalog.pg_tables b where b.schemaname <>'public' and b.schemaname <>'pg_catalog' and b.schemaname <> 'information_schema' and b.schemaname<>'gp_toolkit';"
    # sql="select * from gp_biao_column_to_row limit 10;"
    conn=getConn(db_name)
    for dd in readDb(conn, sql):
        print dd['schemaname']+'.'+dd['tablename']


    # starttime = time.time()
    # # pool = ThreadPool(6) 
    # pool = Pool(36) 
    # pool.map(convert, readDb(conn, sql)) 
    # pool.close() 
    # pool.join()
    # endtime = time.time() - starttime
    # print endtime 
    
    # for dd in data:
    #     print dd
    
    # data = ('huawei','huawei','huawei')
    # sql="insert into public.parse_result (proc_name,tar_name,src_name) values (%s,%s,%s);"
    # writeDb(conn,sql,data)
    conn.close()

    # print data
    # for item in data:
    #     print item['typ']
    #     print "\n"
    # print getConf("hisdb")


if __name__ == '__main__':
    main()
