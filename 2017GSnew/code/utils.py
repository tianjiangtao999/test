# -*- coding: utf-8 -*-
import os,sys
from psycopg2 import pool
import logging
import traceback
dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
error_log =dirname + os.sep + "tt.log"
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=error_log,
                filemode='a')
def get_path(folder):
    for fpathe, dirs, fs in os.walk(folder):
        for f in fs:
            if (f[-2:]) == 'pl':
                # print os.path.join(fpathe, f), "\n"
                yield os.path.join(fpathe, f)

def getConf(dbname='postgres'):
    global dirname
    from ConfigParser import ConfigParser
    conFile = dirname + os.sep + "database.ini"
    conFig = ConfigParser()
    conFig.read(conFile)
    return (conFig.get(dbname, 'database'), conFig.get(dbname, 'user'), conFig.get(dbname, 'password'), conFig.get(dbname, 'host'), conFig.get(dbname, 'port'))

def initDbPool(dbname,minc=1,maxc=1):
    (database, db_user, db_pwd, db_host, db_port) = getConf(dbname)
    try:
        dbpool = pool.ThreadedConnectionPool(minconn=minc,
            maxconn=maxc,
            database=database,
            user=db_user,
            password=db_pwd,
            host=db_host,
            port=db_port
        )
        return dbpool
    except Exception , e :
        logging.error('数据库连接池获取失败:%s' % e)
        
    

def writeDb(connPool,sqls):
    """
    连接数据库（写），并进行写的操作，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回true
    """
    
    conn = getConn(connPool)
    cursor = conn.cursor()
    try:
        for sql in sqls:
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
#增加对ddl的支持，ddl执行失败不能回滚，否则等于没有执行，直接报错就行了，与dml不同    
def writeDbDDL(connPool,sqls):
    """
    连接数据库（写），并进行写的操作，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回true
    """
    
    conn = getConn(connPool)
    cursor = conn.cursor()
    try:
        for sql in sqls:
            cursor.execute(sql)
        conn.commit()  # 提交事务
        return True
    except Exception, e:
        # conn.rollback()  # 如果出错，则事务回滚
        logging.error('数据写入失败:%s' % e)
        return False
    finally:
        cursor.close()
        connPool.putconn(conn)
    




def ReadDbOnceReturn(connPool,sql):
    """
    连接数据库（从），并进行数据查询，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回查询到的数据，这个数据是经过转换的，转成字典格式，方便模板调用，其中字典的key是数据表里的字段名
    """
    conn = getConn(connPool)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        data = [dict((cursor.description[i][0], value) for i, value in enumerate(row))  \
        for row in cursor.fetchall()]  # 转换数据，字典格式

        return data
    except Exception, e:
        logging.error('具体的报错%s' % traceback.format_exc())
        logging.error('sql执行失败:%s' % e)
        
    finally:
        cursor.close()
        conn.commit()
        connPool.putconn(conn)

def ReadDbBulkReturn(connPool, sql, bulkc=10):
    """
    连接数据库（从），并进行数据查询，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回查询到的数据，这个数据是经过转换的，转成字典格式，方便模板调用，其中字典的key是数据表里的字段名
    """
    conn = getConn(connPool)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        data = [dict((cursor.description[i][0], value) for i, value in enumerate(row))  \
        for row in cursor.fetchall()]  # 转换数据，字典格式
        n=bulkc
        for tempdata in [data[i:i+n] for i in range(0, len(data), n)]:
            yield tempdata
    except Exception, e:
        logging.error('sql执行失败:%s' % e)
        
    finally:
        cursor.close()
        conn.commit()
        connPool.putconn(conn)

    
    
def getConn(dbPool):
    try:
        conn = dbPool.getconn()
        # print conn
        return conn
    except Exception, e:
        print type(e),e
        logging.error('''数据库连接失败:%s''' % e)
        exit(-1)

def syncG2p(srcPool,destPool,getSql,putSql,strFormat):
    # print "GetTableStatic"
    # global connPool
    # starttime = time.time()
    InsertData=[]
    if getSql:
        for lines in ReadDbOnceReturn(srcPool,getSql):
            # print map(lambda x :lines[x],strFormat)
            #如果不对lines进行转换，可能会因为包含''导致写入失败，因为有的是int，强制转成str
            datasqls= putSql % tuple(map(lambda x: str(lines[x]).replace("'","''"),strFormat))
            InsertData.append(datasqls)
            
        
        writeDb(destPool,InsertData)
        # print InsertData
        
        # endtime = time.time() - starttime
        # utils.logging.info('the daily job  spend time is %.2f' % endtime)
    else:
        InsertData.append(putSql)
        writeDb(destPool,InsertData)