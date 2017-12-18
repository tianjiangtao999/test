use strict;
use warnings;
use Net::FTP;
use IO::Callback;
use Time::Local;
use Redis;

my $BytesPerHash = 10240;
my $count        = 0;
my ( $ptime, $ctime, $psize, $csize, $tsize, $percent, $speed ) =   ( time(), time(), 0, 0, 0, 0, 0 );
my ($remotefile,$localfile,$offset);

# current size, speed, percent
my $HashHandle = IO::Callback->new('>',	sub {
		$count++;
		$csize   = $offset + $BytesPerHash * $count;
		$csize   = ( $csize > $tsize ? $tsize : $csize );
		$percent = ( $tsize == 0 ? 1.0 : $csize * 1.0 / $tsize );
		$percent = ( $percent > 1.0 ? 1.0 : $percent );
		$percent = sprintf( "%.4f", $percent );
		$ctime   = time();
		if ( $ctime - $ptime >= 1 ) {
			$speed = ( $csize - $psize ) / ( $ctime - $ptime );
			print "#### current size $csize B\n";
			print "### speed $speed B/s\n";
			$psize = $csize;
			$ptime = $ctime;
			print "## percent $percent\n";
			printredis($remotefile,$tsize,$csize,$speed,$percent);
		}
		else {
			if ( $csize == $tsize ) {
				$speed = 0;
				print "#### current size $csize B\n";
				print "### speed $speed B/s\n";
				print "## percent $percent\n";
				printredis($remotefile,$tsize,$csize,$speed,$percent);
			}
		}
	}
);

# support to resume transferring from beakpoint
sub ftpdownload {
	my $ftp = Net::FTP->new( $_[0], BlockSize => $BytesPerHash, Debug => 0 ) or die "FTP cannot connect to host: $!";
	$ftp->login( $_[1], $_[2] ) or die "FTP cannot login ", $ftp->message;
	$ftp->binary;
	$tsize = $ftp->size( $_[3] );
	print "# total size $tsize\n";
	$ftp->hash( $HashHandle, $BytesPerHash );
	$offset = (stat($localfile))[7] || 0;
	if($offset eq 0)
	{
		print "New downloading\n";
	}
	else
	{
		print "Resume downloading from $offset B\n";
	}
	$ftp->get( $_[3], $_[4], $offset ) or die "FTP cannot download ", $ftp->message;
	$ftp->quit();
	return 0;
}

# print to redis
sub printredis {
	my ($filename,$tsize,$csize,$speed,$percent)=@_;
	my $redis = Redis->new( server => '127.0.0.1:6379' );
	$redis->hmset( $filename, TotalSize=>"$tsize", CurrentSize=>"$csize", CurrentSpeed=>"$speed", ProcessPercent=>"$percent");
	$redis->quit();
}

# example    perl frr.pl /usr/local/games/src.txt /root/script/dest.txt
sub main {
	if($#ARGV<0){ 
		print "[ERROR] Please run as 'perl $0 remotefile localfile'\n"; 
		return -1;
	}
	($remotefile,$localfile)=($ARGV[0],$ARGV[1]);
	print `date`;
	print "Start ftp download ...\n";
	my $ret = ftpdownload( '127.0.0.1', 'root', 'root',	$remotefile ,$localfile);
	print `date`;
	if ( $ret != 0 ) {
		return -2;
	}
	return 0;
}

my $result = main();
exit($result);
