use strict;
use warnings;
use Net::FTP;
use IO::Callback;
use Time::Local;
my $BytesPerHash = 10240;
my $count = 0;
my ($ptime,$ctime,$psize,$csize,$tsize,$percent,$speed)=(time(),time(),0, 0, 0, 0, 0 );
my $HashHandle = IO::Callback->new('>',
        sub {
                $count++;
                $csize=$BytesPerHash*$count;
                $csize=($csize>$tsize?$tsize:$csize);
                $percent=($tsize==0?1.0:$csize*1.0/$tsize);
                $percent=($percent>1.0?1.0:$percent);
                $percent=sprintf("%.4f",$percent);
                $ctime=time();
                if ($ctime-$ptime>=1) {
                        #ctime是当前时间，ptime是上次开始下载时间，如果时间大于1说明有新的更新
                    $speed=($csize-$psize)/($ctime-$ptime);
                    print "#### current size $csize B\n";
                    print "### speed $speed B/s\n";
                    $psize=$csize;
                    $ptime=$ctime;
                    print "## percent $percent\n";
                }
                else{
                 if($csize==$tsize)
                 {
                  $speed=0;
                  print "#### current size $csize B\n";
                  print "### speed $speed B/s\n";
                  print "## percent $percent\n";
                 }
                }
        }
);
sub ftpdownload
{
        my $ftp = Net::FTP->new($_[0],BlockSize => $BytesPerHash, Debug => 0) or die "FTP cannot connect to host: $!";
        $ftp->login($_[1],$_[2]) or die "FTP cannot login ", $ftp->message;
        $ftp->binary;
        $tsize = $ftp->size($_[3]);
        print "# total size $tsize\n";
        $ftp->hash($HashHandle, $BytesPerHash);
        #注意BytesPerHash和BlockSize必须一样，否则两个count会不一样
        $ftp->get($_[3]) or die "FTP cannot download ",$ftp->message;
        $ftp->quit();
        return 0;
}
sub main
{
        print "Start ftp download ...\n";
        my $ret=ftpdownload('192.168.6.128','root','root','/usr/local/games/SYSBAK.MAX');
        if($ret!=0)
        {
                return -2;
        }
        return 0;
}
my $result=main();
exit($result);
 
执行结果：
[root@nagios script]# perl f.pl
Start ftp download ...
# total size 2097147556
#### current size 82841600 B
### speed 82841600 B/s
## percent 0.0395
#### current size 195358720 B
### speed 112517120 B/s
## percent 0.0932
#### current size 326768640 B
### speed 131409920 B/s
## percent 0.1558
#### current size 461905920 B
### speed 135137280 B/s
## percent 0.2203
#### current size 587888640 B
### speed 125982720 B/s
## percent 0.2803
#### current size 696616960 B
### speed 108728320 B/s
## percent 0.3322
#### current size 773785600 B
### speed 77168640 B/s
## percent 0.3690
#### current size 925900800 B
### speed 152115200 B/s
## percent 0.4415
#### current size 1060198400 B
### speed 134297600 B/s
## percent 0.5055
#### current size 1166233600 B
### speed 106035200 B/s
## percent 0.5561
#### current size 1270824960 B
### speed 104591360 B/s
## percent 0.6060
#### current size 1346488320 B
### speed 75663360 B/s
## percent 0.6421
#### current size 1522534400 B
### speed 176046080 B/s
## percent 0.7260
#### current size 1644625920 B
### speed 122091520 B/s
## percent 0.7842
#### current size 1751162880 B
### speed 106536960 B/s
## percent 0.8350
#### current size 1857945600 B
### speed 106782720 B/s
## percent 0.8859
#### current size 1934663680 B
### speed 76718080 B/s
## percent 0.9225
#### current size 2097147556 B
### speed 0 B/s
## percent 1.0000