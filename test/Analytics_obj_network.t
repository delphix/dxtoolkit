use strict;
use Data::Dumper;
use Test::More tests => 6;
use Test::Output;

use lib '../lib';
use Analytic_obj;
use Engine;
use server;

# #stop server
# my $ua = LWP::UserAgent->new;
# $ua->agent("Delphix Perl Agent/0.1");
# $ua->timeout(15);
# my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
# my $response = $ua->request($request);

my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();

#$server->run();


my $debug ;

my $FD = \*STDOUT;

my @args = ('1.4',$debug);
my $engine = new_ok( 'Engine' =>  \@args , 'Create Engine object');
ok($engine->load_config('test.conf') eq 0,'load config');
ok($engine->dlpx_connect('test') eq 0,'connect to test engine');

my @axis = ( 'latency','throughput','count','op','client' );
my @args = ( $engine, 'nfs-by-client', 'nfs-by-client', 'type', \@axis , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'nfs-by-client - network');

$obj->getData('','');
$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency
2015-03-27 18:01:00,172.16.180.143,1.79,1.79,3.57,0.05,N/A
2015-03-27 18:02:00,172.16.180.143,1.69,1.79,3.47,0.05,N/A
2015-03-27 18:03:00,172.16.180.143,1.70,1.79,3.49,0.05,N/A
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "nfs - by client - network");

my @axis = ( 'latency','throughput','count','op','client', 'cached' );
my @args = ( $engine, 'nfs-all', 'nfs-all', 'type', \@axis , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'nfs-all - network');

$obj->getData('','');
$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency,read_cache_hit_ratio
2015-04-03 12:10:45,172.16.180.143,5.72,0.00,5.73,7.28,7.28,64.29
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "nfs-all - network");



my @axis = ( 'latency','throughput','count','op','client', 'cached' );
my @args = ( $engine, 'nfs-all', 'nfs-all-nocache', 'type', \@axis , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'nfs-all-nocache - network');
$obj->getData('','');
$obj->processData(10);

my $output_line = <<'END_LINE';
#timestamp,client,read_throughput,write_throughput,total_throughput,read_latency,write_latency,read_cache_hit_ratio
2015-04-03 13:22:53,172.16.180.143,0.00,1.00,1.00,N/A,1.50,0
2015-04-03 13:22:54,172.16.180.143,2.00,1.00,3.00,0.25,2.50,100.00
END_LINE

stdout_is ( sub { $obj->print($FD); } , $output_line, "nfs - all - network - case 2");


my @axis = ( 'latency','throughput','count','op','client', 'cached', 'size' );
my @args = ( $engine, 'nfs-all', 'nfs-all', 'type', \@axis , 1 );
my $obj = new_ok( 'Analytic_io_obj' =>  \@args , 'nfs-all - network');

my @axis1 = ( 'inBytes','outBytes','inPackets','outPackets' );
my @args1 = ( $engine, 'default.network', 'network', 'type', \@axis1 , 1 );
my $obj1 = new_ok( 'Analytic_network_obj' =>  \@args1 , 'network - network');

$obj->getData('','');
$obj->processData(10);
$obj1->getData('','');
$obj1->processData(10, $obj);


my $output_line = <<'END_LINE';
#timestamp,inBytes,outBytes,vdb_write,vdb_read
2015-04-03 12:10:45,18042,17910,N/A,131072
END_LINE

stdout_is ( sub { $obj1->print($FD); } , $output_line, "nfs - all - network");

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);