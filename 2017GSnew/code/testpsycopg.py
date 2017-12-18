# -*- coding: gbk -*-
import psycopg2
import logging
import pprint
db_name = "postgres"
db_user = "postgres"
db_pass = "postgres"
db_ip = "133.96.72.167"
error_log =  "/home/tt.log"
# 定义日志输出格式
logging.basicConfig(level=logging.ERROR,format = '%(asctime)s %(filename)s[line:%lineno)d] %(levelname)s %(message)s',datefmt = '%Y-%m-%d %H:%M:%S',filename = error_log,filemode = 'a')

def writeDb(sql,data):
    """
    连接mysql数据库（写），并进行写的操作，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回true
    """
    try:
        conn = psycopg2.connect(database=db_name,user=db_user,password=db_pass,host=db_ip,port=5432)
        cursor = conn.cursor()
    except Exception,e:
        print e
        logging.error('数据库连接失败:%s' % e)
        return False
    try:
        cursor.execute(sql,data)
        conn.commit()   #提交事务
    except Exception,e:
        conn.rollback()   #如果出错，则事务回滚
        logging.error('数据写入失败:%s' % e)
        return False
    finally:
        cursor.close()
        conn.close()
    return True
def readDb(sql):
    """
    连接mysql数据库（从），并进行数据查询，如果连接失败，会把错误写入日志中，并返回false，如果sql执行失败，也会把错误写入日志中，并返回false，如果所有执行正常，则返回查询到的数据，这个数据是经过转换的，转成字典格式，方便模板调用，其中字典的key是数据表里的字段名
    """
    try:
        conn = psycopg2.connect(database=db_name,user=db_user,password=db_pass,host=db_ip,port=5432)
        cursor = conn.cursor()
    except Exception,e:
        print e
        logging.error('数据库连接失败:%s' % e)
        return False
    try:
        cursor.execute(sql)
        data = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]     #转换数据，字典格式
    except Exception,e:
        logging.error('数据执行失败:%s' % e)
        return False
    finally:
        cursor.close()
        conn.close()
    return data

def main():
    sql="select count(*)  from gp_biao_column_to_row limit 10;"
    data=readDb(sql)
    
    print data
    # for item in data:
    #     print item['typ']
    #     print "\n"
    

    
if __name__=='__main__':
    main()
