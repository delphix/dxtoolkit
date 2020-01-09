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


script_compiles('../../bin/dx_get_capacity.pl');

script_runs(['../../bin/dx_get_capacity.pl', '-d', 'local', '-format','csv','-group','G5','-details','all'] ,  "get capacity on group");

my $expected_stdout = <<EOF;
#Engine,Group,Database,Replica,Size [GB],Type,Size [GB],Snapshots,Size [GB]
local,G5,DB5P,NO,   4502.15,,,,
,,,,,Current copy,   3204.33,,
,,,,,DB Logs,     33.41,,
,,,,,Snapshots total,   1264.41,,
,,,,,,,Snapshots shared,    403.32
,,,,,,,Snapshot 2017-05-13T08:53:38.481Z,    404.46
,,,,,,,Snapshot 2017-07-19T07:50:12.549Z,    123.42
,,,,,,,Snapshot 2017-07-23T07:43:19.600Z,    122.55
,,,,,,,Snapshot 2017-09-14T07:43:27.907Z,    165.39
,,,,,,,Snapshot 2017-10-03T07:52:57.546Z,     22.73
,,,,,,,Snapshot 2017-10-04T07:50:03.107Z,      5.22
,,,,,,,Snapshot 2017-10-05T07:51:54.440Z,      5.27
,,,,,,,Snapshot 2017-10-06T07:59:23.156Z,      5.26
,,,,,,,Snapshot 2017-10-07T07:59:28.265Z,      3.81
,,,,,,,Snapshot 2017-10-08T07:31:47.413Z,      2.98
,,,,,,,Snapshot 2017-10-09T07:31:58.777Z,      0.00
local,G5,VDB5A,NO,    292.99,,,,
,,,,,Current copy,    268.54,,
,,,,,DB Logs,      1.24,,
,,,,,Snapshots total,     21.08,,
,,,,,,,Snapshots shared,     10.77
,,,,,,,Snapshot 2017-10-02T22:30:04.790Z,      3.44
,,,,,,,Snapshot 2017-10-03T22:30:04.999Z,      1.73
,,,,,,,Snapshot 2017-10-04T22:30:05.555Z,      1.12
,,,,,,,Snapshot 2017-10-05T22:30:04.044Z,      1.21
,,,,,,,Snapshot 2017-10-06T22:30:04.280Z,      0.98
,,,,,,,Snapshot 2017-10-07T22:30:04.953Z,      1.06
,,,,,,,Snapshot 2017-10-08T22:30:04.409Z,      0.77
local,G5,VDB5C,NO,    256.43,,,,
,,,,,Current copy,    233.14,,
,,,,,DB Logs,      1.75,,
,,,,,Snapshots total,     18.62,,
,,,,,,,Snapshots shared,      1.96
,,,,,,,Snapshot 2017-08-01T18:19:09.158Z,     12.00
,,,,,,,Snapshot 2017-10-02T22:30:03.141Z,      1.19
,,,,,,,Snapshot 2017-10-03T22:30:02.863Z,      0.76
,,,,,,,Snapshot 2017-10-04T22:30:02.764Z,      0.74
,,,,,,,Snapshot 2017-10-05T22:30:02.398Z,      0.75
,,,,,,,Snapshot 2017-10-06T22:30:02.711Z,      0.65
,,,,,,,Snapshot 2017-10-07T22:30:06.711Z,      0.41
,,,,,,,Snapshot 2017-10-08T22:30:03.955Z,      0.09
,,,,,,,Snapshot 2017-10-09T03:57:15.183Z,      0.09
local,G5,VDB5Q,NO,    191.39,,,,
,,,,,Current copy,    174.25,,
,,,,,DB Logs,      0.43,,
,,,,,Snapshots total,     14.61,,
,,,,,,,Snapshots shared,      9.53
,,,,,,,Snapshot 2017-10-02T22:30:04.036Z,      1.20
,,,,,,,Snapshot 2017-10-03T22:30:05.223Z,      0.72
,,,,,,,Snapshot 2017-10-04T22:30:05.380Z,      0.72
,,,,,,,Snapshot 2017-10-05T22:30:04.110Z,      0.75
,,,,,,,Snapshot 2017-10-06T22:30:04.484Z,      0.72
,,,,,,,Snapshot 2017-10-07T22:30:05.252Z,      0.70
,,,,,,,Snapshot 2017-10-08T22:30:04.490Z,      0.28
local,G5,VDB5R,NO,    213.96,,,,
,,,,,Current copy,    202.23,,
,,,,,DB Logs,      1.39,,
,,,,,Snapshots total,      8.73,,
,,,,,,,Snapshots shared,      4.07
,,,,,,,Snapshot 2017-10-02T22:30:03.136Z,      1.22
,,,,,,,Snapshot 2017-10-03T22:30:02.900Z,      0.59
,,,,,,,Snapshot 2017-10-04T22:30:02.823Z,      0.84
,,,,,,,Snapshot 2017-10-05T22:30:02.731Z,      0.79
,,,,,,,Snapshot 2017-10-06T22:30:02.709Z,      0.60
,,,,,,,Snapshot 2017-10-07T22:30:03.922Z,      0.31
,,,,,,,Snapshot 2017-10-08T22:30:03.822Z,      0.30
EOF

script_stdout_is $expected_stdout, "get capacity on group results compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
