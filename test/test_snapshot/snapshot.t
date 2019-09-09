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
script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-name','Oracle dsource'] ,  "dSource snapshot test");

my $expected_stdout = <<EOF;
#Engine,Group,Database,Snapshot name,Start time,End time
local,Sources,Oracle dsource,\@2017-05-09T18:14:12.639Z,2017-05-09 14:14:08 EDT,2017-05-09 14:14:08 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:00:04.096Z,2017-06-06 07:59:59 EDT,2017-06-06 08:18:29 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:18:28.754Z,2017-06-06 08:18:29 EDT,2017-06-06 08:54:00 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:54:00.857Z,2017-06-06 08:54:00 EDT,2017-06-06 08:58:12 EDT
EOF

script_stdout_is $expected_stdout, "dSource snapshot results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead'] ,  "All objects");

my $expected_stdout = <<EOF;
local,Analytics,autofs,\@2017-04-24T11:30:00.178,2017-04-24 11:30:00 IST,2017-04-24 11:30:00 IST
local,Analytics,autotest,\@2017-05-12T21:04:31.870,2017-05-12 21:04:31 IST,2017-05-12 21:04:31 IST
local,Analytics,mstest_time,\@2017-04-24T14:33:00.180,2017-04-24 14:33:00 BST,2017-04-24 14:33:00 BST
local,Analytics,si4rac,\@2017-05-25T13:30:53.559Z,2017-05-25 14:30:53 IST,2017-05-25 23:01:39 IST
local,Analytics,siclone,\@2017-05-11T11:42:50.764Z,2017-05-11 12:42:50 IST,2017-05-11 12:42:51 IST
local,Analytics,targetcon,\@2017-05-22T15:35:03.206Z,2017-05-22 16:35:02 IST,2017-05-22 16:35:09 IST
local,Sources,AdventureWorksLT2008R2,\@2017-04-24T06:25:44.000,2017-04-24 06:25:44 PDT,2017-04-24 06:28:54 PDT
local,Sources,AdventureWorksLT2008R2,\@2017-04-24T06:28:54.000,2017-04-24 06:28:54 PDT,2017-04-24 06:28:54 PDT
local,Sources,Oracle dsource,\@2017-05-09T18:14:12.639Z,2017-05-09 14:14:08 EDT,2017-05-09 14:14:08 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:00:04.096Z,2017-06-06 07:59:59 EDT,2017-06-06 08:18:29 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:18:28.754Z,2017-06-06 08:18:29 EDT,2017-06-06 08:54:00 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:54:00.857Z,2017-06-06 08:54:00 EDT,2017-06-06 08:58:12 EDT
local,Sources,PDB,\@2017-05-22T15:17:45.106Z,2017-05-22 11:17:44 EDT,2017-05-22 11:17:44 EDT
local,Sources,racdba,\@2016-12-23T13:24:08.203Z,2016-12-23 13:23:44 UTC,2016-12-23 13:23:56 UTC
local,Sources,singpdb,\@2017-03-17T12:23:34.209Z,2017-03-17 08:23:25 EDT,2017-03-17 08:24:01 EDT
local,Sources,singpdb,\@2017-04-04T14:11:13.784Z,2017-04-04 10:11:12 EDT,2017-05-22 11:14:24 EDT
local,Sources,singpdb,\@2017-05-22T15:14:25.413Z,2017-05-22 11:14:24 EDT,2017-05-22 11:17:44 EDT
local,Sources,Sybase dsource,\@2017-05-05T14:43:00.000,2017-05-05 14:43:00 EDT,2017-05-05 14:43:00 EDT
local,Sources,test,\@2017-06-06T20:56:15.244Z,2017-06-06 21:56:15 GMT+01:00,2017-06-06 21:56:36 GMT+01:00
local,Sources,test,\@2017-06-06T20:56:36.799Z,2017-06-06 21:56:36 GMT+01:00,2017-06-07 11:30:01 GMT+01:00
local,Sources,test,\@2017-06-07T10:30:01.056Z,2017-06-07 11:30:01 GMT+01:00,2017-06-08 11:30:03 GMT+01:00
local,Sources,test,\@2017-06-08T10:30:02.277Z,2017-06-08 11:30:03 GMT+01:00,2017-06-09 11:30:01 GMT+01:00
local,Sources,test,\@2017-06-09T10:30:01.161Z,2017-06-09 11:30:01 GMT+01:00,2017-06-10 11:30:05 GMT+01:00
local,Sources,test,\@2017-06-10T10:30:04.000Z,2017-06-10 11:30:05 GMT+01:00,2017-06-11 11:30:01 GMT+01:00
local,Sources,test,\@2017-06-11T10:30:01.091Z,2017-06-11 11:30:01 GMT+01:00,2017-06-12 11:30:03 GMT+01:00
local,Sources,test,\@2017-06-12T10:30:02.638Z,2017-06-12 11:30:03 GMT+01:00,2017-06-13 11:30:01 GMT+01:00
local,Sources,test,\@2017-06-13T10:30:01.097Z,2017-06-13 11:30:01 GMT+01:00,2017-06-13 14:29:59 GMT+01:00
local,Sources,TESTEBI,\@2017-04-06T13:16:37.405,2017-04-06 13:16:37 IST,2017-04-06 13:16:37 IST
local,Sources,TESTEBI,\@2017-05-10T15:39:31.846,2017-05-10 15:39:31 IST,2017-05-10 15:39:31 IST
local,Sources,TESTEBI,\@2017-05-12T11:30:01.826,2017-05-12 11:30:01 IST,2017-05-12 11:30:01 IST
local,Sources,TESTEBI,\@2017-05-13T13:02:06.974,2017-05-13 13:02:06 IST,2017-05-13 13:02:06 IST
local,Sources,TESTEBI,\@2017-05-25T14:30:49.342,2017-05-25 14:30:49 IST,2017-05-25 14:30:49 IST
local,Tests,VOracledsource_F0C,\@2017-06-06T13:14:35.860Z,2017-06-06 09:14:36 EDT,2017-06-06 09:14:41 EDT
EOF

script_stdout_is $expected_stdout, "All objects results compare";

script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-startDate','2017-06-06 07:59:59'] ,  "startDate");

my $expected_stdout = <<EOF;
local,Sources,Oracle dsource,\@2017-06-06T12:00:04.096Z,2017-06-06 07:59:59 EDT,2017-06-06 08:18:29 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:18:28.754Z,2017-06-06 08:18:29 EDT,2017-06-06 08:54:00 EDT
local,Sources,Oracle dsource,\@2017-06-06T12:54:00.857Z,2017-06-06 08:54:00 EDT,2017-06-06 08:58:12 EDT
local,Tests,VOracledsource_F0C,\@2017-06-06T13:14:35.860Z,2017-06-06 09:14:36 EDT,2017-06-06 09:14:41 EDT
EOF

script_stdout_is $expected_stdout, "startDate results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-endDate','2017-05-13 00:00:00','-name','TESTEBI'] ,  "endDate plus name");
my $expected_stdout = <<EOF;
local,Sources,TESTEBI,\@2017-04-06T13:16:37.405,2017-04-06 13:16:37 IST,2017-04-06 13:16:37 IST
local,Sources,TESTEBI,\@2017-05-10T15:39:31.846,2017-05-10 15:39:31 IST,2017-05-10 15:39:31 IST
local,Sources,TESTEBI,\@2017-05-12T11:30:01.826,2017-05-12 11:30:01 IST,2017-05-12 11:30:01 IST
EOF

script_stdout_is $expected_stdout, "endDate plus name results compare";


script_runs(['../../bin/dx_get_snapshots.pl', '-d', 'local', '-format','csv','-nohead','-details','-name','autofs,autotest,mstest_time,si4rac,siclone,targetcon'] ,  "Names and details");

my $expected_stdout = <<EOF;
local,Analytics,autofs,\@2017-04-24T11:30:00.178,2017-04-24 11:30:00 IST,2017-04-24 11:30:00 IST,2017-04-24 11:30:00 IST,current,Policy,N/A
local,Analytics,autotest,\@2017-05-12T21:04:31.870,2017-05-12 21:04:31 IST,2017-05-12 21:04:31 IST,2017-05-13 13:01:35 IST,current,Policy,15.7 SP101
local,Analytics,mstest_time,\@2017-04-24T14:33:00.180,2017-04-24 14:33:00 BST,2017-04-24 14:33:00 BST,2017-04-24 14:33:10 BST,current,123,10.50.1600.1
local,Analytics,si4rac,\@2017-05-25T13:30:53.559Z,2017-05-25 14:30:53 IST,2017-05-25 23:01:39 IST,2017-05-25 14:30:53 IST,current,Policy,12.1.0.2.0
local,Analytics,siclone,\@2017-05-11T11:42:50.764Z,2017-05-11 12:42:50 IST,2017-05-11 12:42:51 IST,2017-05-11 12:42:50 IST,current,Policy,12.1.0.2.0
local,Analytics,targetcon,\@2017-05-22T15:35:03.206Z,2017-05-22 16:35:02 IST,2017-05-22 16:35:09 IST,2017-05-22 16:35:03 IST,current,Policy,12.1.0.2.0
EOF

script_stdout_is $expected_stdout, "Names and details results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
