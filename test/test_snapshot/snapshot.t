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
script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-name','PDBX1'] ,  "dSource snapshot test");

my $expected_stdout = <<EOF;
#Engine,Group,Database,Snapshot name,Start time,End time
local,Sources,PDBX1,\@2019-12-30T11:21:28.097Z,2019-12-30 03:21:29 PST,2019-12-30 03:21:29 PST
local,Sources,PDBX1,\@2019-12-30T11:31:23.253Z,2019-12-30 03:31:23 PST,2019-12-30 03:31:23 PST
local,Sources,PDBX1,\@2020-01-05T11:31:40.649Z,2020-01-05 03:31:40 PST,2020-01-21 06:34:07 PST
EOF

script_stdout_is $expected_stdout, "dSource snapshot results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead'] ,  "All objects");

my $expected_stdout = <<EOF;
local,Analytics,cdbkate,\@2019-12-30T11:36:26.484Z,2019-12-30 03:36:20 PST,2019-12-30 04:08:15 PST
local,Analytics,oratest,\@2020-01-05T11:30:10.307Z,2020-01-05 03:30:12 PST,2020-01-05 23:30:06 PST
local,Analytics,pdbtest,\@2019-12-30T11:36:33.724Z,2019-12-30 03:36:40 PST,2019-12-30 04:07:51 PST
local,Analytics,pdbtest2,\@2020-01-05T11:30:29.078Z,2020-01-05 03:30:36 PST,2020-01-21 00:00:49 PST
local,Analytics,piorovdb,\@2020-01-15T03:30:10.490,2020-01-15 03:30:10 PST,2020-01-15 03:30:10 PST
local,Analytics,piorovdb,\@2020-01-16T03:30:00.443,2020-01-16 03:30:00 PST,2020-01-16 03:30:00 PST
local,Analytics,piorovdb,\@2020-01-17T03:30:08.573,2020-01-17 03:30:08 PST,2020-01-17 03:30:08 PST
local,Analytics,piorovdb,\@2020-01-18T03:30:09.370,2020-01-18 03:30:09 PST,2020-01-18 03:30:09 PST
local,Analytics,piorovdb,\@2020-01-19T03:30:08.943,2020-01-19 03:30:08 PST,2020-01-19 03:30:08 PST
local,Analytics,piorovdb,\@2020-01-20T03:30:10.040,2020-01-20 03:30:10 PST,2020-01-20 03:30:10 PST
local,Analytics,piorovdb,\@2020-01-21T03:30:10.563,2020-01-21 03:30:10 PST,2020-01-21 03:30:10 PST
local,Analytics,vcdbtest,\@2020-01-05T11:30:16.445Z,2020-01-05 03:30:23 PST,2020-01-21 00:00:49 PST
local,Sources,carmel,\@2019-12-30T11:17:06.880Z,2019-12-30 03:17:03 PST,2019-12-30 03:21:29 PST
local,Sources,carmel,\@2019-12-30T11:30:39.990Z,2019-12-30 03:30:39 PST,2019-12-30 03:31:23 PST
local,Sources,carmel,\@2020-01-05T11:31:14.326Z,2020-01-05 03:31:12 PST,2020-01-21 06:34:07 PST
local,Sources,marina,\@2019-12-30T11:21:04.276Z,2019-12-30 03:20:58 PST,2019-12-30 03:20:58 PST
local,Sources,marina,\@2020-01-05T11:31:14.181Z,2020-01-05 03:31:09 PST,2020-01-05 03:31:12 PST
local,Sources,PDBX1,\@2019-12-30T11:21:28.097Z,2019-12-30 03:21:29 PST,2019-12-30 03:21:29 PST
local,Sources,PDBX1,\@2019-12-30T11:31:23.253Z,2019-12-30 03:31:23 PST,2019-12-30 03:31:23 PST
local,Sources,PDBX1,\@2020-01-05T11:31:40.649Z,2020-01-05 03:31:40 PST,2020-01-21 06:34:07 PST
local,Sources,rockets,\@2020-01-13T09:21:28.223,2020-01-13 09:21:28 PST,2020-01-13 09:21:28 PST
EOF

script_stdout_is $expected_stdout, "All objects results compare";

script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-startDate','2020-01-05 00:00:08'] ,  "startDate");

my $expected_stdout = <<EOF;
local,Analytics,oratest,\@2020-01-05T11:30:10.307Z,2020-01-05 03:30:12 PST,2020-01-05 23:30:06 PST
local,Analytics,pdbtest2,\@2020-01-05T11:30:29.078Z,2020-01-05 03:30:36 PST,2020-01-21 00:00:49 PST
local,Analytics,piorovdb,\@2020-01-15T03:30:10.490,2020-01-15 03:30:10 PST,2020-01-15 03:30:10 PST
local,Analytics,piorovdb,\@2020-01-16T03:30:00.443,2020-01-16 03:30:00 PST,2020-01-16 03:30:00 PST
local,Analytics,piorovdb,\@2020-01-17T03:30:08.573,2020-01-17 03:30:08 PST,2020-01-17 03:30:08 PST
local,Analytics,piorovdb,\@2020-01-18T03:30:09.370,2020-01-18 03:30:09 PST,2020-01-18 03:30:09 PST
local,Analytics,piorovdb,\@2020-01-19T03:30:08.943,2020-01-19 03:30:08 PST,2020-01-19 03:30:08 PST
local,Analytics,piorovdb,\@2020-01-20T03:30:10.040,2020-01-20 03:30:10 PST,2020-01-20 03:30:10 PST
local,Analytics,piorovdb,\@2020-01-21T03:30:10.563,2020-01-21 03:30:10 PST,2020-01-21 03:30:10 PST
local,Analytics,vcdbtest,\@2020-01-05T11:30:16.445Z,2020-01-05 03:30:23 PST,2020-01-21 00:00:49 PST
local,Sources,carmel,\@2020-01-05T11:31:14.326Z,2020-01-05 03:31:12 PST,2020-01-21 06:34:07 PST
local,Sources,marina,\@2020-01-05T11:31:14.181Z,2020-01-05 03:31:09 PST,2020-01-05 03:31:12 PST
local,Sources,PDBX1,\@2020-01-05T11:31:40.649Z,2020-01-05 03:31:40 PST,2020-01-21 06:34:07 PST
local,Sources,rockets,\@2020-01-13T09:21:28.223,2020-01-13 09:21:28 PST,2020-01-13 09:21:28 PST
EOF

script_stdout_is $expected_stdout, "startDate results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-endDate','2020-01-18 00:00:08','-name','piorovdb'] ,  "endDate plus name");
my $expected_stdout = <<EOF;
local,Analytics,piorovdb,\@2020-01-15T03:30:10.490,2020-01-15 03:30:10 PST,2020-01-15 03:30:10 PST
local,Analytics,piorovdb,\@2020-01-16T03:30:00.443,2020-01-16 03:30:00 PST,2020-01-16 03:30:00 PST
local,Analytics,piorovdb,\@2020-01-17T03:30:08.573,2020-01-17 03:30:08 PST,2020-01-17 03:30:08 PST
EOF

script_stdout_is $expected_stdout, "endDate plus name results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-details','-name','rockets,PDBX1,oratest'] ,  "Names and details");

my $expected_stdout = <<EOF;
local,Analytics,oratest,\@2020-01-05T11:30:10.307Z,2020-01-05 03:30:12 PST,2020-01-05 23:30:06 PST,2020-01-05 03:30:10 PST,current,Policy,12.1.0.2.0
local,Sources,PDBX1,\@2019-12-30T11:21:28.097Z,2019-12-30 03:21:29 PST,2019-12-30 03:21:29 PST,2019-12-30 03:21:28 PST,current,Policy,12.1.0.2.0
local,Sources,PDBX1,\@2019-12-30T11:31:23.253Z,2019-12-30 03:31:23 PST,2019-12-30 03:31:23 PST,2019-12-30 03:31:23 PST,current,Policy,12.1.0.2.0
local,Sources,PDBX1,\@2020-01-05T11:31:40.649Z,2020-01-05 03:31:40 PST,2020-01-21 06:34:07 PST,2020-01-05 03:31:40 PST,current,Policy,12.1.0.2.0
local,Sources,rockets,\@2020-01-13T09:21:28.223,2020-01-13 09:21:28 PST,2020-01-13 09:21:28 PST,2020-01-13 09:23:09 PST,current,Policy,15.7 SP137
EOF

script_stdout_is $expected_stdout, "Names and details results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
