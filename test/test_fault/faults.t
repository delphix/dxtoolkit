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

 
script_compiles('../../bin/dx_get_faults.pl');
script_runs(['../../bin/dx_get_faults.pl', '-d', 'local', '-format','csv','-st','2017-12-27 16:00:00'] ,  "all faults test");

my $expected_stdout = <<EOF;
#Appliance,Fault ref,Status,Date Diagnosed,Severity,Target,Title
local,FAULT-16,RESOLVED,2017-12-27 16:04:19 GMT,WARNING,test/sdd,Hook operations failed to complete successfully
local,FAULT-17,ACTIVE,2017-12-28 11:30:00 GMT,WARNING,Sources/Vtes_X6F,Unable to connect to remote database during virtual database policy enforcement
local,FAULT-18,ACTIVE,2017-12-28 11:30:01 GMT,WARNING,test/sdd,Unable to connect to remote database during virtual database policy enforcement
local,FAULT-19,RESOLVED,2018-01-09 03:01:09 GMT,CRITICAL,Sources/carmel,LogSync failed to connect to remote database
local,FAULT-20,ACTIVE,2018-01-09 03:01:18 GMT,WARNING,Sources/carmel,Cannot provision a database from a portion of TimeFlow
local,FAULT-21,RESOLVED,2018-01-20 01:24:45 GMT,CRITICAL,Sources/carmel,LogSync failed to connect to remote database
local,FAULT-22,ACTIVE,2018-01-23 11:30:02 GMT,CRITICAL,Sources/PDBX1,Unable to connect to remote database during dSource policy enforcement
local,FAULT-23,ACTIVE,2018-01-23 11:30:03 GMT,CRITICAL,Sources/marina,Unable to connect to remote database during dSource policy enforcement
EOF

script_stdout_is $expected_stdout, "faults results compare";

script_runs(['../../bin/dx_get_faults.pl', '-d', 'local', '-format','csv','-st','2017-12-27 16:00:00','-status','RESOLVED'] ,  "RESOLVED faults test");

my $expected_stdout = <<EOF;
#Appliance,Fault ref,Status,Date Diagnosed,Severity,Target,Title
local,FAULT-16,RESOLVED,2017-12-27 16:04:19 GMT,WARNING,test/sdd,Hook operations failed to complete successfully
local,FAULT-19,RESOLVED,2018-01-09 03:01:09 GMT,CRITICAL,Sources/carmel,LogSync failed to connect to remote database
local,FAULT-21,RESOLVED,2018-01-20 01:24:45 GMT,CRITICAL,Sources/carmel,LogSync failed to connect to remote database
EOF

script_stdout_is $expected_stdout, "RESOLVED results compare";



#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
