# -*- coding: gbk -*-
import psycopg2
import logging
import pprint
db_name = "postgres"
db_user = "postgres"
db_pass = "postgres"
db_ip = "133.96.72.167"
error_log =  "/home/tt.log"
# ������־�����ʽ
logging.basicConfig(level=logging.ERROR,format = '%(asctime)s %(filename)s[line:%lineno)d] %(levelname)s %(message)s',datefmt = '%Y-%m-%d %H:%M:%S',filename = error_log,filemode = 'a')

def writeDb(sql,data):
    """
    ����mysql���ݿ⣨д����������д�Ĳ������������ʧ�ܣ���Ѵ���д����־�У�������false�����sqlִ��ʧ�ܣ�Ҳ��Ѵ���д����־�У�������false���������ִ���������򷵻�true
    """
    try:
        conn = psycopg2.connect(database=db_name,user=db_user,password=db_pass,host=db_ip,port=5432)
        cursor = conn.cursor()
    except Exception,e:
        print e
        logging.error('���ݿ�����ʧ��:%s' % e)
        return False
    try:
        cursor.execute(sql,data)
        conn.commit()   #�ύ����
    except Exception,e:
        conn.rollback()   #�������������ع�
        logging.error('����д��ʧ��:%s' % e)
        return False
    finally:
        cursor.close()
        conn.close()
    return True
def readDb(sql):
    """
    ����mysql���ݿ⣨�ӣ������������ݲ�ѯ���������ʧ�ܣ���Ѵ���д����־�У�������false�����sqlִ��ʧ�ܣ�Ҳ��Ѵ���д����־�У�������false���������ִ���������򷵻ز�ѯ�������ݣ���������Ǿ���ת���ģ�ת���ֵ��ʽ������ģ����ã������ֵ��key�����ݱ�����ֶ���
    """
    try:
        conn = psycopg2.connect(database=db_name,user=db_user,password=db_pass,host=db_ip,port=5432)
        cursor = conn.cursor()
    except Exception,e:
        print e
        logging.error('���ݿ�����ʧ��:%s' % e)
        return False
    try:
        cursor.execute(sql)
        data = [dict((cursor.description[i][0], value) for i, value in enumerate(row)) for row in cursor.fetchall()]     #ת�����ݣ��ֵ��ʽ
    except Exception,e:
        logging.error('����ִ��ʧ��:%s' % e)
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
