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
local,group1,vcdb2,\@2017-10-10T14:01:41.692Z,2017-10-10 07:01:41 PDT,   0.00203,
local,Sources,marina,\@2017-10-10T11:06:21.139Z,2017-10-10 04:06:21 PDT,   0.00125,group1/test/previous tf
local,Sources,marina,\@2017-10-10T11:34:35.638Z,2017-10-10 04:34:35 PDT,   0.00112,group1/testdx/current tf;group1/test/previous tf
local,group1,testdx,\@2017-10-10T11:40:08.525Z,2017-10-10 04:40:08 PDT,   0.00094,
local,group1,test,\@2017-10-10T12:12:07.084Z,2017-10-10 05:12:07 PDT,   0.00089,
local,group1,vcdb,\@2017-10-10T13:52:56.670Z,2017-10-10 06:52:56 PDT,   0.00084,
local,group1,test,\@2017-10-10T11:40:15.163Z,2017-10-10 04:40:15 PDT,   0.00071,
local,group1,vcdb,\@2017-10-10T13:56:20.207Z,2017-10-10 06:56:20 PDT,   0.00068,
local,group1,vcdb,\@2017-10-10T13:54:56.527Z,2017-10-10 06:54:56 PDT,   0.00044,group1/vcdb2/current tf
local,group1,vdb2,\@2017-10-10T14:01:50.865Z,2017-10-10 07:01:50 PDT,   0.00016,
local,group1,test,\@2017-10-10T12:07:59.135Z,2017-10-10 05:07:59 PDT,   0.00010,
local,group1,vpdb,\@2017-10-10T13:56:27.391Z,2017-10-10 06:56:27 PDT,   0.00009,
local,group1,vpdb,\@2017-10-10T13:55:03.953Z,2017-10-10 06:55:03 PDT,   0.00009,group1/vdb2/current tf
local,group1,vpdb,\@2017-10-10T13:53:06.026Z,2017-10-10 06:53:06 PDT,   0.00009,
local,Sources,cdbkate,\@2017-10-10T13:46:32.118Z,2017-10-10 06:46:32 PDT,   0.00000,group1/vcdb/current tf
local,Sources,marina,\@2017-10-10T12:05:21.936Z,2017-10-10 05:05:21 PDT,   0.00000,group1/test/current tf
local,Sources,PDBKATE,\@2017-10-10T13:47:21.166Z,2017-10-10 06:47:21 PDT,   0.00000,group1/vpdb/current tf
EOF

script_stdout_is $expected_stdout, "get snapshot on group results compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
