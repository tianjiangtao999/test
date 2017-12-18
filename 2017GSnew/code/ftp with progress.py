#python实现的百分比,来源于英文文档，修改了其中的问题
from ftplib import FTP
import signal
import os.path
import time

#signal.signal(signal.SIGALRM, uploadTracker.handle(1024))
#signal.alarm(1)

class FtpUploadTracker:
    sizeWritten = 0
    totalSize = 0
    lastShownPercent = 0
    percentComplete  =0
    def __init__(self, totalSize):
        self.totalSize = totalSize
        
    def handle(self, block):
        self.lastShownPercent = self.percentComplete
        self.sizeWritten += 1024
        self.percentComplete = round((float(self.sizeWritten)/ self.totalSize) * 100,1)
        if (float(self.lastShownPercent) != float(self.percentComplete)):
            print self.percentComplete,self.lastShownPercent
            self.lastShownPercent = self.percentComplete

#Open FTP connection
ftp = FTP('192.168.232.129')
ftp.login('ftpuser','ftpuser')

filename ='hello.txt'
totalSize = os.path.getsize(filename)
print('Total file size : ' + str(round(totalSize / 1024 / 1024 ,1)) + ' Mb')
# Open the file and upload it
file = open(filename, 'rb')
uploadTracker = FtpUploadTracker(int(totalSize))
ftp.storbinary('STOR '+filename, file, 2048,uploadTracker.handle)
#ftp.storbinary('STOR '+filename, file, 1024, uploadTracker.handle)
# Close the connection and the file
ftp.quit()
file.close()