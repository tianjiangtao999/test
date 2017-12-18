import logging
import pprint
# import psycopg2
from psycopg2 import pool
import time
from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool
'''
用来测试psycopg2的数据库连接池
'''

def getConf(dbname='postgres'):
    from ConfigParser import ConfigParser
    conFile = r"./database.ini"
    conFig = ConfigParser()
    conFig.read(conFile)
    return (conFig.get(dbname, 'database'), conFig.get(dbname, 'user'), conFig.get(dbname, 'password'), conFig.get(dbname, 'host'), conFig.get(dbname, 'port'))
dbname='hisdb'
(database, db_user, db_pwd, db_host, db_port) = getConf(dbname)
dbpool = pool.ThreadedConnectionPool(
            minconn=4,
            maxconn=100,
            database=database,
            user=db_user,
            password=db_pwd,
            host=db_host,
            port=db_port
        )
conn = dbpool.getconn()
cur = conn.cursor()
sql="select * from dis.tc_table_space_tj_20170707_d limit 10;"
cur.execute(sql)
print cur.fetchall()
cur.close()
conn.commit()
dbpool.putconn(conn)
dbpool.closeall()
