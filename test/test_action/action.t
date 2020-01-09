use strict;
use Data::Dumper;
use Test::More tests => 5;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;



my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();

 
script_compiles('../../bin/dx_get_audit.pl');

script_runs(['../../bin/dx_get_audit.pl', '-d', 'local', '-format','csv','-st','2019-12-25 00:00:00'] ,  "get audit data");

my $expected_stdout = <<EOF;
#Appliance,StartTime,State,User or Policy,Type,Details
local,2019-12-30 03:06:43 PST,COMPLETED,admin,USER_LOGIN,Log in as user "admin" from IP "10.43.17.25".
local,2019-12-30 03:08:45 PST,COMPLETED,admin,USER_LOGIN,Log in as user "admin" from IP "10.43.17.25".
local,2019-12-30 03:08:53 PST,COMPLETED,admin,GROUP_CREATE,Create group "Sources".
local,2019-12-30 03:08:56 PST,COMPLETED,admin,USER_LOGIN,Log in as user "admin" from IP "172.16.124.26".
local,2019-12-30 03:09:03 PST,COMPLETED,admin,DB_LINK,Link dSource "marina" from source "marina".
local,2019-12-30 03:09:07 PST,COMPLETED,admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from "localhost".
local,2019-12-30 03:09:08 PST,COMPLETED,admin,SOURCE_CONFIG_UPDATE,Update source config "marina".
local,2019-12-30 03:09:09 PST,COMPLETED,admin,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:13:07 PST,COMPLETED,admin,SOURCE_CONFIG_UPDATE,Update source config "carmel".
local,2019-12-30 03:13:17 PST,COMPLETED,admin,DB_LINK,Link dSource "PDBX1" from source "PDBX1".
local,2019-12-30 03:13:28 PST,COMPLETED,admin,DB_SYNC,Run SnapSync for database "PDBX1".
local,2019-12-30 03:13:30 PST,COMPLETED,admin,DB_SYNC,Run SnapSync for database "carmel".
local,2019-12-30 03:14:20 PST,FAILED,admin,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:14:29 PST,FAILED,admin,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:14:47 PST,COMPLETED,admin,USER_LOGOUT,Log out user "admin".
local,2019-12-30 03:14:56 PST,FAILED,admin,USER_FAILED_LOGIN,Failed attempt to log in as user "admin" from IP "172.16.124.26".
local,2019-12-30 03:15:08 PST,COMPLETED,admin,USER_LOGIN,Log in as user "admin" from IP "172.16.124.26".
local,2019-12-30 03:15:21 PST,COMPLETED,admin,MASKINGJOB_FETCH,Fetching all Masking Jobs from "localhost".
local,2019-12-30 03:19:38 PST,COMPLETED,admin,DB_DETACH_SOURCE,Detach source "marina" from database "marina".
local,2019-12-30 03:19:50 PST,COMPLETED,admin,DB_DETACH_SOURCE,Detach source "PDBX1" from database "PDBX1".
local,2019-12-30 03:19:56 PST,COMPLETED,admin,SOURCE_DISABLE,Disable dataset "carmel".
local,2019-12-30 03:20:04 PST,COMPLETED,admin,DB_ATTACH_SOURCE,Attach source "marina" to database "marina".
local,2019-12-30 03:20:18 PST,COMPLETED,admin,DB_ATTACH_SOURCE,Attach source "PDBX1" to database "PDBX1".
local,2019-12-30 03:20:30 PST,COMPLETED,admin,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:21:06 PST,COMPLETED,admin,DB_SYNC,Run SnapSync for database "PDBX1".
local,2019-12-30 03:21:07 PST,COMPLETED,admin,SOURCE_ENABLE,Enable dataset "carmel".
local,2019-12-30 03:21:35 PST,COMPLETED,admin,GROUP_CREATE,Create group "Analytics".
local,2019-12-30 03:21:36 PST,COMPLETED,admin,DB_PROVISION,Provision virtual database "oratest".
local,2019-12-30 03:25:24 PST,COMPLETED,admin,DB_SYNC,Run SnapSync for database "oratest".
local,2019-12-30 03:25:35 PST,COMPLETED,admin,SOURCE_CONFIG_UPDATE,Update source config "cdbkate".
local,2019-12-30 03:25:40 PST,WAITING,admin,DB_PROVISION,Provision virtual database "pdbtest".
local,2019-12-30 03:26:11 PST,COMPLETED,admin,DB_LINK,Link dSource "cdbkate" from source "cdbkate".
local,2019-12-30 03:30:00 PST,COMPLETED,default snapsync,DB_SYNC,Run SnapSync for database "PDBX1".
local,2019-12-30 03:30:00 PST,COMPLETED,default snapsync,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:30:00 PST,COMPLETED,default snapshot,DB_SYNC,Run SnapSync for database "oratest".
local,2019-12-30 03:30:02 PST,COMPLETED,default snapsync,DB_SYNC,Run SnapSync for database "carmel".
local,2019-12-30 03:31:31 PST,COMPLETED,admin,DB_DELETE,Delete dataset "CPDBTESTKMPd".
local,2019-12-30 03:31:32 PST,COMPLETED,admin,SOURCE_STOP,Stop dataset "CPDBTESTKMPd".
local,2019-12-30 03:31:56 PST,COMPLETED,admin,CAPACITY_RECLAMATION,Space is being reclaimed.
local,2019-12-30 03:32:12 PST,WAITING,admin,DB_SYNC,Run SnapSync for database "pdbtest".
local,2019-12-30 03:32:18 PST,WAITING,admin,DB_SYNC,Run SnapSync for database "cdbkate".
EOF

script_stdout_is $expected_stdout, "get audit results compare";


script_runs(['../../bin/dx_get_audit.pl', '-d', 'local', '-format','csv','-state','FAILED'] ,  "get audit data with state");

my $expected_stdout = <<EOF;
#Appliance,StartTime,State,User or Policy,Type,Details
local,2019-12-30 03:14:20 PST,FAILED,admin,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:14:29 PST,FAILED,admin,DB_SYNC,Run SnapSync for database "marina".
local,2019-12-30 03:14:56 PST,FAILED,admin,USER_FAILED_LOGIN,Failed attempt to log in as user "admin" from IP "172.16.124.26".
EOF

script_stdout_is $expected_stdout, "get audit with state results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
