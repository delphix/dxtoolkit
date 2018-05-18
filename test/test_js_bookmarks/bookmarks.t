use strict;
use Data::Dumper;
use Test::More tests => 11;
use Test::Script;
use LWP::UserAgent;
use lib '/Users/mprzepiorowski/Documents/oss_dxtoolkit/dxtoolkit/lib/';
use lib '/Users/mprzepiorowski/Documents/oss_dxtoolkit/dxtoolkit/test/';
use lib '.';
use server;




my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();

 
script_compiles('../../bin/dx_get_js_bookmarks.pl');
script_runs(['../../bin/dx_get_js_bookmarks.pl', '-d', 'local', '-format','csv'] ,  "all js bookmarks test");

my $expected_stdout = <<EOF;
#Appliance,Bookmark name,Bookmark time,Template name,Container name,Branch name
local,test2,2018-01-25 16:21:46 GMT,ora,N/A,master
local,test1,2018-01-25 16:47:13 GMT,ora,cont1,default
EOF

script_stdout_is $expected_stdout, "all js bookmarks results compare";

script_runs(['../../bin/dx_get_js_bookmarks.pl', '-d', 'local', '-format','csv','-realtime','-bookmark_name','test1'] ,  "realtime for container test");

my $expected_stdout = <<EOF;
#Appliance,Bookmark name,Bookmark time,Template name,Container name,Branch name,Source name,Source time
local,test1,2018-01-25 16:47:13 GMT,ora,cont1,default,,
,,,,,,testdx,2018-01-25 16:47:12 GMT
EOF

script_stdout_is $expected_stdout, "realtime for container results compare";

script_runs(['../../bin/dx_get_js_bookmarks.pl', '-d', 'local', '-format','csv','-realtime','-bookmark_name','test2'] ,  "realtime for template test");

my $expected_stdout = <<EOF;
#Appliance,Bookmark name,Bookmark time,Template name,Container name,Branch name,Source name,Source time
local,test2,2018-01-25 16:21:46 GMT,ora,N/A,master,,
,,,,,,marina,2018-01-22 11:31:33 GMT
EOF

script_stdout_is $expected_stdout, "realtime for template results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
