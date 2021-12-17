use strict;
use Data::Dumper;
use Test::More tests => 3;
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

script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-size','desc'] ,  "get snapshot size");

my $expected_stdout = <<EOF;
#Engine,Group,Database,Snapshot name,Creation time ,Size,Depended objects
local,Analytics,vcdbtest,\@2021-12-16T16:53:42.376Z,2021-12-16 11:53:42 EST,   0.00397,
local,Analytics,CDOMLOTG2E25,\@2021-12-16T16:53:57.716Z,2021-12-16 11:53:57 EST,   0.00212,
local,Analytics,CDOMLOTG2E25,\@2021-12-16T17:21:06.619Z,2021-12-16 12:21:06 EST,   0.00211,
local,Analytics,CDOMLOTG2E25,\@2021-12-16T16:56:51.683Z,2021-12-16 11:56:51 EST,   0.00199,
local,Analytics,CDOMLOTG2E25,\@2021-12-16T17:14:16.926Z,2021-12-16 12:14:16 EST,   0.00194,
local,Analytics,CDOMLOTG2E25,\@2021-12-16T17:07:25.666Z,2021-12-16 12:07:25 EST,   0.00184,
local,Analytics,vcdbtest,\@2021-12-16T17:29:37.735Z,2021-12-16 12:29:37 EST,   0.00140,
local,Analytics,vcdbtest,\@2021-12-16T17:08:57.839Z,2021-12-16 12:08:57 EST,   0.00111,
local,Analytics,vcdbtest,\@2021-12-16T17:22:45.605Z,2021-12-16 12:22:45 EST,   0.00107,
local,Analytics,vcdbtest,\@2021-12-16T17:15:41.135Z,2021-12-16 12:15:41 EST,   0.00103,
local,Analytics,oratest,\@2021-12-16T16:48:03.588Z,2021-12-16 11:48:03 EST,   0.00092,
local,Analytics,pdbtest2,\@2021-12-16T16:53:51.150Z,2021-12-16 11:53:51 EST,   0.00081,
local,Analytics,pdbtest,\@2021-12-16T17:21:15.607Z,2021-12-16 12:21:15 EST,   0.00077,
local,Analytics,pdbtest2,\@2021-12-16T17:22:54.932Z,2021-12-16 12:22:54 EST,   0.00076,
local,Analytics,pdbtest,\@2021-12-16T17:07:43.325Z,2021-12-16 12:07:43 EST,   0.00075,
local,Analytics,pdbtest,\@2021-12-16T16:54:05.002Z,2021-12-16 11:54:05 EST,   0.00074,
local,Analytics,pdbtest2,\@2021-12-16T17:09:06.665Z,2021-12-16 12:09:06 EST,   0.00074,
local,Analytics,pdbtest2,\@2021-12-16T17:15:48.734Z,2021-12-16 12:15:48 EST,   0.00074,
local,Analytics,vcdbtest,\@2021-12-16T16:58:18.521Z,2021-12-16 11:58:18 EST,   0.00074,
local,Analytics,pdbtest,\@2021-12-16T17:14:25.881Z,2021-12-16 12:14:25 EST,   0.00072,
local,Analytics,pdbtest,No snapshot data,N/A - timezone unknown,   0.00067,
local,Analytics,pdbtest2,No snapshot data,N/A - timezone unknown,   0.00067,
local,Analytics,pdbtest,\@2021-12-16T17:28:23.847Z,2021-12-16 12:28:23 EST,   0.00057,
local,Analytics,vcdbtest,\@2021-12-16T16:58:43.062Z,2021-12-16 11:58:43 EST,   0.00054,
local,Analytics,oratest,\@2021-12-16T17:20:50.465Z,2021-12-16 12:20:50 EST,   0.00049,
local,Analytics,oratest,\@2021-12-16T17:06:30.498Z,2021-12-16 12:06:30 EST,   0.00048,
local,Analytics,oratest,\@2021-12-16T17:23:20.117Z,2021-12-16 12:23:20 EST,   0.00043,
local,Analytics,oratest,\@2021-12-16T17:09:12.336Z,2021-12-16 12:09:12 EST,   0.00042,
local,Sources,Macaroon,\@2021-12-16T08:42:12.000,2021-12-16 08:49:02 PST,   0.00038,
local,Sources,Macaroon,\@2021-12-16T08:53:14.000,2021-12-16 08:53:59 PST,   0.00036,
local,Analytics,pdbtest2,\@2021-12-16T17:29:45.024Z,2021-12-16 12:29:45 EST,   0.00035,
local,Analytics,vcdbtest,\@2021-12-16T17:00:00.073Z,2021-12-16 12:00:00 EST,   0.00035,
local,Analytics,vcdbtest,\@2021-12-16T17:01:18.486Z,2021-12-16 12:01:18 EST,   0.00031,
local,Sources,DBOMSR3A85E9,\@2021-12-16T16:44:14.762Z,2021-12-16 11:44:14 EST,   0.00031,
local,Analytics,oratest,\@2021-12-16T16:52:05.657Z,2021-12-16 11:52:05 EST,   0.00029,
local,Analytics,oratest,\@2021-12-16T16:50:55.401Z,2021-12-16 11:50:55 EST,   0.00029,
local,Analytics,oratest,\@2021-12-16T16:53:28.768Z,2021-12-16 11:53:28 EST,   0.00026,
local,Analytics,oratest,\@2021-12-16T17:09:23.693Z,2021-12-16 12:09:23 EST,   0.00024,
local,Analytics,oratest,\@2021-12-16T17:18:24.085Z,2021-12-16 12:18:24 EST,   0.00024,
local,Analytics,oratest,\@2021-12-16T17:13:47.598Z,2021-12-16 12:13:47 EST,   0.00024,
local,Analytics,oratest,\@2021-12-16T17:27:56.272Z,2021-12-16 12:27:56 EST,   0.00024,
local,Analytics,oratest,\@2021-12-16T17:28:04.898Z,2021-12-16 12:28:04 EST,   0.00024,
local,Analytics,oratest,\@2021-12-16T17:32:04.013Z,2021-12-16 12:32:04 EST,   0.00024,
local,Analytics,oratest,\@2021-12-16T17:23:41.135Z,2021-12-16 12:23:41 EST,   0.00023,
local,Analytics,oratest,\@2021-12-16T17:21:02.649Z,2021-12-16 12:21:02 EST,   0.00023,
local,Analytics,oratest,\@2021-12-16T17:06:57.478Z,2021-12-16 12:06:57 EST,   0.00023,
local,Analytics,oratest,\@2021-12-16T17:18:32.022Z,2021-12-16 12:18:32 EST,   0.00022,
local,Analytics,oratest,\@2021-12-16T17:13:57.621Z,2021-12-16 12:13:57 EST,   0.00022,
local,Analytics,oratest,\@2021-12-16T17:09:34.778Z,2021-12-16 12:09:34 EST,   0.00022,
local,Analytics,oratest,\@2021-12-16T17:32:11.141Z,2021-12-16 12:32:11 EST,   0.00022,
local,Analytics,oratest,\@2021-12-16T17:00:42.895Z,2021-12-16 12:00:42 EST,   0.00021,
local,Analytics,oratest,\@2021-12-16T17:04:08.909Z,2021-12-16 12:04:08 EST,   0.00021,
local,Analytics,pdbtest,\@2021-12-16T16:59:56.622Z,2021-12-16 11:59:56 EST,   0.00021,
local,Analytics,pdbtest2,\@2021-12-16T17:01:25.197Z,2021-12-16 12:01:25 EST,   0.00020,
local,Analytics,pdbtest2,\@2021-12-16T17:00:08.173Z,2021-12-16 12:00:08 EST,   0.00020,
local,Analytics,pdbtest,\@2021-12-16T16:57:02.901Z,2021-12-16 11:57:02 EST,   0.00018,
local,Analytics,pdbtest2,\@2021-12-16T16:58:25.763Z,2021-12-16 11:58:25 EST,   0.00018,
local,Analytics,pdbtest,\@2021-12-16T16:57:26.048Z,2021-12-16 11:57:26 EST,   0.00017,
local,Sources,CDOMLOSRCA1DPDB1,\@2021-12-16T16:45:08.967Z,2021-12-16 11:45:08 EST,   0.00017,
local,Analytics,pdbtest,\@2021-12-16T16:58:41.030Z,2021-12-16 11:58:41 EST,   0.00013,
local,Analytics,pdbtest2,\@2021-12-16T16:58:49.835Z,2021-12-16 11:58:49 EST,   0.00013,
local,Analytics,oratest,\@2021-12-16T16:56:55.322Z,2021-12-16 11:56:55 EST,   0.00012,
local,Analytics,mssqltest,\@2021-12-16T09:04:09.940,2021-12-16 09:04:24 PST,   0.00010,
local,Analytics,mssqltest,\@2021-12-16T09:21:21.817,2021-12-16 09:21:35 PST,   0.00009,
local,Analytics,mssqltest,\@2021-12-16T09:15:46.407,2021-12-16 09:16:01 PST,   0.00008,
local,Analytics,mssqltest,\@2021-12-16T09:18:29.973,2021-12-16 09:18:45 PST,   0.00008,
local,Analytics,sybasetest,\@2021-12-16T11:43:33.286,2021-12-16 11:43:32 EST,   0.00007,
local,Analytics,sybasetest,\@2021-12-16T11:47:49.380,2021-12-16 11:47:48 EST,   0.00007,
local,Sources,db_rhel83_160_1,\@2021-12-16T11:42:09.533,2021-12-16 11:42:33 EST,   0.00007,
local,Analytics,sybasetest,\@2021-12-16T11:50:11.716,2021-12-16 11:50:11 EST,   0.00006,
local,Analytics,sybasetest,\@2021-12-16T11:48:58.230,2021-12-16 11:48:57 EST,   0.00005,
local,Analytics,mssqltest,\@2021-12-16T09:10:12.187,2021-12-16 09:10:25 PST,   0.00004,
local,Analytics,mssqltest,\@2021-12-16T09:08:35.390,2021-12-16 09:08:51 PST,   0.00004,
local,Analytics,mssqltest,\@2021-12-16T09:11:45.407,2021-12-16 09:11:59 PST,   0.00004,
local,Sources,db_rhel83_160_1,\@2021-12-16T11:43:03.720,2021-12-16 11:43:18 EST,   0.00004,Analytics/sybasetest/previous tf
local,Analytics,sybasetest,\@2021-12-16T11:45:25.200,2021-12-16 11:45:24 EST,   0.00003,
local,Analytics,sybasetest,\@2021-12-16T11:44:21.766,2021-12-16 11:44:20 EST,   0.00003,
local,Analytics,sybasetest,\@2021-12-16T11:46:28.466,2021-12-16 11:46:27 EST,   0.00003,
local,Analytics,pdbtest,No snapshot data,N/A - timezone unknown,   0.00002,
local,Analytics,pdbtest2,No snapshot data,N/A - timezone unknown,   0.00002,
local,Analytics,pdbtest,No snapshot data,N/A - timezone unknown,   0.00001,
local,Analytics,pdbtest,No snapshot data,N/A - timezone unknown,   0.00001,
local,Analytics,pdbtest,No snapshot data,N/A - timezone unknown,   0.00001,
local,Analytics,pdbtest2,No snapshot data,N/A - timezone unknown,   0.00001,
local,Analytics,pdbtest2,No snapshot data,N/A - timezone unknown,   0.00001,
local,Sources,CDOMLOSRCA1D,\@2021-12-16T16:44:17.752Z,2021-12-16 11:44:17 EST,   0.00001,Analytics/vcdbtest/current tf
local,Analytics,CDOMLOTG2E25,\@2021-12-16T17:28:14.579Z,2021-12-16 12:28:14 EST,   0.00000,
local,Analytics,mssqltest,\@2021-12-16T09:24:13.447,2021-12-16 09:24:28 PST,   0.00000,
local,Analytics,pdbtest2,No snapshot data,N/A - timezone unknown,   0.00000,
local,Sources,CDOMLOSRCA1DPDB1,\@2021-12-16T16:46:28.246Z,2021-12-16 11:46:28 EST,   0.00000,Analytics/pdbtest2/current tf;Analytics/pdbtest/current tf
local,Sources,DBOMSR3A85E9,\@2021-12-16T16:45:28.371Z,2021-12-16 11:45:28 EST,   0.00000,Analytics/oratest/previous tf
local,Sources,Macaroon,\@2021-12-16T08:55:18.000,2021-12-16 09:01:43 PST,   0.00000,Analytics/mssqltest/current tf
EOF

script_stdout_is $expected_stdout, "get snapshot on group results compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
