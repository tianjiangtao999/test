# -*- coding: gbk -*-
import psycopg2,traceback
import random
import time
import os

'''
create table alf_test 
(id serial NOT NULL
,name character(32)
,cnt  integer
,flag character(5)
,date_time  character(20)
,ospid       character(32)
);
insert into alf_test(name,cnt,flag) values
('lywtgs',0,'0');
'''

def curDate(fmt='%Y-%m-%d %H:%M:%S'):
    return time.strftime(fmt,time.localtime())

def connectAnyDB(dbname=''):
    conn = psycopg2.connect(database='postgres', user='postgres', password='postgres', host='localhost', port='5432')
    return conn

def getChar():
    seed = "abcdefghijklmnopqrstuvwxyz"
    sa = []
    for i in range(5):
        sa.append(random.choice(seed))
        salt = ''.join(sa)
    return salt

def gernDataDom(length):
    sql="insert into alf_test(name,cnt,flag) values"
    for i in range(length):
        sql +="('%s',0,'0')," %(getChar())
    sql=sql.rstrip(',')
    # 拼一个串
    conn   =connectAnyDB('test')
    c=conn.cursor()
    c.execute(sql)
    c.close()
    #游标没有commit函数
    conn.commit()
    conn.close()

def updateInfoFlag(conn,id,flag,pid):
    c=conn.cursor()
    try:
        dtime=curDate(fmt='%Y-%m-%d %H:%M:%S')
        sql='''update alf_test
               set flag='%s',date_time='%s',cnt=cnt+1,ospid='%s'
               where id =%s'''%(flag,dtime,pid,id)
        print sql
        c.execute(sql)
        c.close()
    except Exception:
        traceback.print_exc()

def getConfInfo(conn,pid=None):
    c=conn.cursor()
    if not pid:
        sql = """select id,name,cnt,flag  from alf_test
               where flag='0'
               limit 20
               for update skip locked"""
    else:
        sql="""select id,name,cnt,flag from alf_test
               where flag='1'
               and ospid ='%s'
               for update skip locked""" % pid
    print sql
    c.execute(sql)
    results=c.fetchall()
    return results

def getData():
    conn   =connectAnyDB('test')
    results=getConfInfo(conn)
    pid  = str(os.getpid())
    for row in results:
        print row
        id  =row[0]
        name=row[1]
        cnt =row[2]
        flag=row[3]
        flag='1'
        updateInfoFlag(conn,id,flag,pid)
    #更加进程获取中间结果
    pidres=getConfInfo(conn,pid)
    conn.commit()
    conn.close()
    return pidres

def main():
    #create test data
    # gernDataDom(500)
    pidres=getData()
    #执行程序及过程
    #如果下一步成功将flag为2,否则为0,同时将ospid重置
    #step1
    #sql="select * from alf_test where flag='0' order by id desc limit 5 for update skip locked"
    #step2
    #sql="update alf_test set flag='1' where id =16"
    #step3
    #sql="update alf_test set flag='0' where id =16"
    #step4
    #sql="update alf_test set flag='2' where id =16"
    #check data
    #select * from alf_test where cnt >1;

if __name__=='__main__':
    main()
