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

 
script_compiles('../../bin/dx_get_event.pl');
script_runs(['../../bin/dx_get_event.pl', '-d', 'local', '-format','csv','-st','2018-01-25 11:38:00'] ,  "all faults test");

my $expected_stdout = <<EOF;
#Appliance,Alert,Action,Response,Target name,Timestamp,Serverity,Title,Description
local,ALERT-597,N/A,N/A,testdx,2018-01-25 11:38:06 GMT,INFORMATIONAL,Job complete,ORACLE_UPDATE_REDOLOGS job for "testdx" completed successfully.
local,ALERT-598,N/A,N/A,test/testdx,2018-01-25 11:38:09 GMT,INFORMATIONAL,Job complete,DB_ROLLBACK job for "test/testdx" completed successfully.
local,ALERT-599,N/A,N/A,test/testdx,2018-01-25 11:38:17 GMT,INFORMATIONAL,Job complete,DB_SYNC job for "test/testdx" completed successfully.
local,ALERT-600,N/A,N/A,marina,2018-01-25 11:38:18 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_CONTAINER_ATTEMPT_UPDATE_SOURCE job for "marina" completed successfully.
local,ALERT-601,N/A,N/A,marina,2018-01-25 11:38:18 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_CONTAINER_UPDATE_SOURCE job for "marina" completed successfully.
local,ALERT-602,N/A,N/A,cont1,2018-01-25 11:38:18 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_CONTAINER_RESET job for "cont1" completed successfully.
local,ALERT-603,N/A,N/A,cont1,2018-01-25 12:00:44 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_CONTAINER_DELETE job for "cont1" completed successfully.
local,ALERT-604,N/A,N/A,test/testdx,2018-01-25 12:02:34 GMT,INFORMATIONAL,Job complete,DB_SYNC job for "test/testdx" completed successfully.
local,ALERT-605,N/A,N/A,testdx,2018-01-25 12:02:51 GMT,INFORMATIONAL,Job complete,SOURCE_STOP job for "testdx" completed successfully.
local,ALERT-606,N/A,N/A,test/testdx,2018-01-25 12:04:49 GMT,INFORMATIONAL,Job complete,DB_REFRESH job for "test/testdx" completed successfully.
local,ALERT-607,N/A,N/A,test/testdx,2018-01-25 12:04:57 GMT,INFORMATIONAL,Job complete,DB_SYNC job for "test/testdx" completed successfully.
local,ALERT-608,N/A,N/A,marina,2018-01-25 12:04:57 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_CONTAINER_ATTEMPT_UPDATE_SOURCE job for "marina" completed successfully.
local,ALERT-609,N/A,N/A,marina,2018-01-25 12:04:57 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_CONTAINER_UPDATE_SOURCE job for "marina" completed successfully.
local,ALERT-610,N/A,N/A,cont1,2018-01-25 12:04:57 GMT,INFORMATIONAL,Job complete,JETSTREAM_USER_BRANCH_CREATE job for "cont1" completed successfully.
local,ALERT-611,N/A,N/A,cont1,2018-01-25 12:04:57 GMT,INFORMATIONAL,Job complete,JETSTREAM_ADMIN_CONTAINER_CREATE job for "cont1" completed successfully.
local,ALERT-612,N/A,N/A,unknown,2018-01-25 14:36:17 GMT,INFORMATIONAL,Job complete,MASKINGJOB_FETCH job for "unknown" completed successfully.
EOF

script_stdout_is $expected_stdout, "all faults compare";




#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
