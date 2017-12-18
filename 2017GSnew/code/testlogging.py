import logging
import os,sys
dirname, filename = os.path.split(os.path.abspath(sys.argv[0]))
print dirname+os.sep+'\myapp.log'
logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                filename=dirname+os.sep+'myapp.log',
                filemode='a')
    
#logging.debug('This is debug message')
#logging.info('This is info message')
#logging.warning('This is warning message')
def test():
    print __file__
    try:
        f = open(r'd:\test')
        data=f.readlines()
    except Exception,e:
        logging.error('This is info message %s' % e)


if __name__ == '__main__':
    test()
