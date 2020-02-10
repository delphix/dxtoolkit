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


script_compiles('../../bin/dx_get_event.pl');
script_runs(['../../bin/dx_get_event.pl', '-d', 'local', '-format','csv','-st','2019-12-30 03:18:00'] ,  "all faults test");

my $expected_stdout = <<EOF;
#Appliance,Alert,Action,Response,Target name,Timestamp,Severity,Title,Description
local,ALERT-25,N/A,N/A,Sources/PDBX1,2019-12-30 03:19:25 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Sources/PDBX1" completed successfully.
local,ALERT-26,N/A,N/A,Sources/marina,2019-12-30 03:19:46 PST,INFORMATIONAL,Job complete,DB_DETACH_SOURCE job for "Sources/marina" completed successfully.
local,ALERT-27,N/A,N/A,Sources/PDBX1,2019-12-30 03:19:56 PST,INFORMATIONAL,Job complete,DB_DETACH_SOURCE job for "Sources/PDBX1" completed successfully.
local,ALERT-28,N/A,N/A,carmel,2019-12-30 03:19:56 PST,INFORMATIONAL,Job complete,SOURCE_DISABLE job for "carmel" completed successfully.
local,ALERT-29,N/A,N/A,Sources/marina,2019-12-30 03:20:06 PST,INFORMATIONAL,Job complete,DB_ATTACH_SOURCE job for "Sources/marina" completed successfully.
local,ALERT-30,N/A,N/A,Sources/PDBX1,2019-12-30 03:20:19 PST,INFORMATIONAL,Job complete,DB_ATTACH_SOURCE job for "Sources/PDBX1" completed successfully.
local,ALERT-31,N/A,N/A,Sources/marina,2019-12-30 03:21:04 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Sources/marina" completed successfully.
local,ALERT-32,N/A,N/A,carmel,2019-12-30 03:21:07 PST,INFORMATIONAL,Job complete,SOURCE_ENABLE job for "carmel" completed successfully.
local,ALERT-33,N/A,N/A,Sources/PDBX1,2019-12-30 03:21:33 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Sources/PDBX1" completed successfully.
local,ALERT-34,N/A,N/A,Analytics/oratest,2019-12-30 03:25:33 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Analytics/oratest" completed successfully.
local,ALERT-35,N/A,N/A,Analytics/oratest,2019-12-30 03:25:33 PST,INFORMATIONAL,Job complete,DB_PROVISION job for "Analytics/oratest" completed successfully.
local,ALERT-36,N/A,N/A,Analytics/cdbkate,2019-12-30 03:26:14 PST,INFORMATIONAL,Job complete,DB_LINK job for "Analytics/cdbkate" completed successfully.
local,ALERT-37,N/A,N/A,Analytics/oratest,2019-12-30 03:30:11 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Analytics/oratest" completed successfully.
local,ALERT-38,N/A,N/A,Sources/marina,2019-12-30 03:30:39 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Sources/marina" completed successfully.
local,ALERT-39,N/A,N/A,Sources/carmel,2019-12-30 03:30:51 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Sources/carmel" completed successfully.
local,ALERT-40,N/A,N/A,Sources/PDBX1,2019-12-30 03:31:25 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Sources/PDBX1" completed successfully.
local,ALERT-44,N/A,N/A,system,2019-12-30 03:32:06 PST,INFORMATIONAL,Job complete,CAPACITY_RECLAMATION job completed successfully.
local,ALERT-45,N/A,N/A,Analytics/cdbkate,2019-12-30 03:36:28 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Analytics/cdbkate" completed successfully.
local,ALERT-46,N/A,N/A,Analytics/pdbtest,2019-12-30 03:36:49 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Analytics/pdbtest" completed successfully.
local,ALERT-47,N/A,N/A,Analytics/pdbtest,2019-12-30 03:36:49 PST,INFORMATIONAL,Job complete,DB_PROVISION job for "Analytics/pdbtest" completed successfully.
local,ALERT-48,N/A,N/A,Analytics/vcdbtest,2019-12-30 03:44:27 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Analytics/vcdbtest" completed successfully.
local,ALERT-49,N/A,N/A,Analytics/pdbtest2,2019-12-30 03:44:46 PST,INFORMATIONAL,Job complete,DB_SYNC job for "Analytics/pdbtest2" completed successfully.
local,ALERT-50,N/A,N/A,Analytics/pdbtest2,2019-12-30 03:44:46 PST,INFORMATIONAL,Job complete,DB_PROVISION job for "Analytics/pdbtest2" completed successfully.
local,ALERT-51,N/A,N/A,Sources/PDBX1,2019-12-30 04:06:31 PST,INFORMATIONAL,Job complete,DB_DETACH_SOURCE job for "Sources/PDBX1" completed successfully.
local,ALERT-52,N/A,N/A,carmel,2019-12-30 04:06:31 PST,INFORMATIONAL,Job complete,SOURCE_DISABLE job for "carmel" completed successfully.
local,ALERT-53,N/A,N/A,PDBTEST,2019-12-30 04:08:24 PST,INFORMATIONAL,Job complete,SOURCE_STOP job for "PDBTEST" completed successfully.
local,ALERT-54,N/A,N/A,cdbkate,2019-12-30 04:08:25 PST,INFORMATIONAL,Job complete,SOURCE_DISABLE job for "cdbkate" completed successfully.
local,ALERT-55,N/A,N/A,PDBTEST,2019-12-30 04:08:25 PST,INFORMATIONAL,Job complete,SOURCE_DISABLE job for "PDBTEST" completed successfully.
EOF

script_stdout_is $expected_stdout, "all faults compare";




#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
