use strict;
use Data::Dumper;
use Test::More tests => 11;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;




my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();


script_compiles('../../bin/dx_get_snapshots.pl');
script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-name','CDOMLOSRCA1DPDB1'] ,  "dSource snapshot test");

my $expected_stdout = <<EOF;
#Engine,Group,Database,Snapshot name,Start time,End time
local,Sources,CDOMLOSRCA1DPDB1,\@2021-12-17T10:03:59.330Z,2021-12-17 05:03:56 EST,2021-12-17 05:05:09 EST
local,Sources,CDOMLOSRCA1DPDB1,\@2021-12-17T10:05:10.483Z,2021-12-17 05:05:09 EST,2021-12-17 05:05:09 EST
EOF

script_stdout_is $expected_stdout, "dSource snapshot results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead'] ,  "All objects");

my $expected_stdout = <<EOF;
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:13:05.041Z,2021-12-17 05:12:59 EST,2021-12-17 05:16:17 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:16:21.286Z,2021-12-17 05:16:17 EST,2021-12-17 05:27:05 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:27:08.595Z,2021-12-17 05:27:05 EST,2021-12-17 05:34:00 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:34:02.929Z,2021-12-17 05:34:00 EST,2021-12-17 05:41:11 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:41:17.305Z,2021-12-17 05:41:11 EST,2021-12-17 05:48:22 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:48:27.307Z,2021-12-17 05:48:22 EST,2021-12-17 05:56:53 EST
local,Analytics,mssqltest,\@2021-12-17T02:41:10.760,2021-12-17 02:41:10 PST,2021-12-17 02:41:10 PST
local,Analytics,oratest,\@2021-12-17T10:42:32.284Z,2021-12-17 05:42:33 EST,2021-12-17 05:42:33 EST
local,Analytics,oratest,\@2021-12-17T10:46:52.189Z,2021-12-17 05:46:52 EST,2021-12-17 05:47:01 EST
local,Analytics,oratest,\@2021-12-17T10:47:00.792Z,2021-12-17 05:47:01 EST,2021-12-17 05:51:58 EST
local,Analytics,pdbtest,\@2021-12-17T10:48:35.363Z,2021-12-17 05:48:37 EST,2021-12-17 05:56:53 EST
local,Analytics,pdbtest2,\@2021-12-17T10:49:52.302Z,2021-12-17 05:49:54 EST,2021-12-17 05:49:55 EST
local,Analytics,sybasetest,\@2021-12-17T05:02:16.186,2021-12-17 05:02:16 EST,2021-12-17 05:02:16 EST
local,Analytics,sybasetest,\@2021-12-17T05:03:06.506,2021-12-17 05:03:06 EST,2021-12-17 05:03:06 EST
local,Analytics,sybasetest,\@2021-12-17T05:04:09.590,2021-12-17 05:04:09 EST,2021-12-17 05:04:09 EST
local,Analytics,sybasetest,\@2021-12-17T05:08:54.016,2021-12-17 05:08:54 EST,2021-12-17 05:08:54 EST
local,Analytics,vcdbtest,\@2021-12-17T10:49:44.280Z,2021-12-17 05:49:47 EST,2021-12-17 05:49:55 EST
local,Sources,CDOMLOSRCA1D,\@2021-12-17T10:03:14.859Z,2021-12-17 05:03:07 EST,2021-12-17 05:05:09 EST
local,Sources,CDOMLOSRCA1DPDB1,\@2021-12-17T10:03:59.330Z,2021-12-17 05:03:56 EST,2021-12-17 05:05:09 EST
local,Sources,CDOMLOSRCA1DPDB1,\@2021-12-17T10:05:10.483Z,2021-12-17 05:05:09 EST,2021-12-17 05:05:09 EST
local,Sources,db_rhel83_160_1,\@2021-12-17T05:00:56.550,2021-12-17 05:00:56 EST,2021-12-17 05:00:56 EST
local,Sources,db_rhel83_160_1,\@2021-12-17T05:01:49.583,2021-12-17 05:01:49 EST,2021-12-17 05:01:49 EST
local,Sources,DBOMSR3A85E9,\@2021-12-17T10:03:13.032Z,2021-12-17 05:02:59 EST,2021-12-17 05:03:05 EST
local,Sources,DBOMSR3A85E9,\@2021-12-17T10:04:19.720Z,2021-12-17 05:04:11 EST,2021-12-17 05:04:16 EST
local,Sources,Macaroon,\@2021-12-17T02:00:56.000,2021-12-17 02:00:56 PST,2021-12-17 02:00:56 PST
local,Sources,Macaroon,\@2021-12-17T02:10:35.000,2021-12-17 02:10:35 PST,2021-12-17 02:10:35 PST
local,Sources,Macaroon,\@2021-12-17T02:12:30.000,2021-12-17 02:12:30 PST,2021-12-17 02:12:30 PST
EOF

script_stdout_is $expected_stdout, "All objects results compare";

script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-startDate','2021-12-17 05:30:00'] ,  "startDate");

my $expected_stdout = <<EOF;
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:34:02.929Z,2021-12-17 05:34:00 EST,2021-12-17 05:41:11 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:41:17.305Z,2021-12-17 05:41:11 EST,2021-12-17 05:48:22 EST
local,Analytics,CDOMLOTG2E25,\@2021-12-17T10:48:27.307Z,2021-12-17 05:48:22 EST,2021-12-17 05:56:53 EST
local,Analytics,oratest,\@2021-12-17T10:42:32.284Z,2021-12-17 05:42:33 EST,2021-12-17 05:42:33 EST
local,Analytics,oratest,\@2021-12-17T10:46:52.189Z,2021-12-17 05:46:52 EST,2021-12-17 05:47:01 EST
local,Analytics,oratest,\@2021-12-17T10:47:00.792Z,2021-12-17 05:47:01 EST,2021-12-17 05:51:58 EST
local,Analytics,pdbtest,\@2021-12-17T10:48:35.363Z,2021-12-17 05:48:37 EST,2021-12-17 05:56:53 EST
local,Analytics,pdbtest2,\@2021-12-17T10:49:52.302Z,2021-12-17 05:49:54 EST,2021-12-17 05:49:55 EST
local,Analytics,vcdbtest,\@2021-12-17T10:49:44.280Z,2021-12-17 05:49:47 EST,2021-12-17 05:49:55 EST
EOF

script_stdout_is $expected_stdout, "startDate results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-endDate','2021-12-17 05:03:06','-name','sybasetest'] ,  "endDate plus name");
my $expected_stdout = <<EOF;
local,Analytics,sybasetest,\@2021-12-17T05:02:16.186,2021-12-17 05:02:16 EST,2021-12-17 05:02:16 EST
local,Analytics,sybasetest,\@2021-12-17T05:03:06.506,2021-12-17 05:03:06 EST,2021-12-17 05:03:06 EST
local,Analytics,sybasetest,\@2021-12-17T05:04:09.590,2021-12-17 05:04:09 EST,2021-12-17 05:04:09 EST
local,Analytics,sybasetest,\@2021-12-17T05:07:42.216,2021-12-17 05:07:42 EST,2021-12-17 05:07:42 EST
EOF

script_stdout_is $expected_stdout, "endDate plus name results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-details','-name','sybasetest,oratest'] ,  "Names and details");

my $expected_stdout = <<EOF;
local,Analytics,oratest,\@2021-12-17T10:42:32.284Z,2021-12-17 05:42:33 EST,2021-12-17 05:42:33 EST,2021-12-17 05:42:32 EST,old,Policy,19.3.0.0.0
local,Analytics,oratest,\@2021-12-17T10:46:52.189Z,2021-12-17 05:46:52 EST,2021-12-17 05:47:01 EST,2021-12-17 05:46:52 EST,current,Policy,19.3.0.0.0
local,Analytics,oratest,\@2021-12-17T10:47:00.792Z,2021-12-17 05:47:01 EST,2021-12-17 05:51:58 EST,2021-12-17 05:47:00 EST,current,Policy,19.3.0.0.0
local,Analytics,sybasetest,\@2021-12-17T05:02:16.186,2021-12-17 05:02:16 EST,2021-12-17 05:02:16 EST,2021-12-17 05:02:16 EST,old,Policy,16.0 SP03 PL08
local,Analytics,sybasetest,\@2021-12-17T05:03:06.506,2021-12-17 05:03:06 EST,2021-12-17 05:03:06 EST,2021-12-17 05:03:06 EST,old,Policy,16.0 SP03 PL08
local,Analytics,sybasetest,\@2021-12-17T05:04:09.590,2021-12-17 05:04:09 EST,2021-12-17 05:04:09 EST,2021-12-17 05:04:09 EST,old,Policy,16.0 SP03 PL08
local,Analytics,sybasetest,\@2021-12-17T05:08:54.016,2021-12-17 05:08:54 EST,2021-12-17 05:08:54 EST,2021-12-17 05:08:54 EST,current,Policy,16.0 SP03 PL08
EOF

script_stdout_is $expected_stdout, "Names and details results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
