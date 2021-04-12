use strict;
use Data::Dumper;
use Test::More tests => 13;
use Test::Script;
use Test::Files;
use File::Spec;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;




my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();


script_compiles('../../bin/dx_get_db_env.pl');

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv',  "list databases"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Parent snapshot,Used(GB),Status,Enabled,Unique Name,Parent time,VDB creation time
local,10.110.247.60,CDOMLOSRCA1D,Sources,CDB,,N/A,1.30,RUNNING,enabled,CDOMLOSRCA1D,N/A,2021-04-09 07:52:20
local,10.110.247.60,CDOMLOSRCA1DPDB1,Sources,dSource,,N/A,0.23,RUNNING,enabled,N/A,N/A,2021-04-09 07:52:20
local,NA,CDOMLOSRCA1DPDB2,Sources,detached,,N/A,0.23,NA,N/A,N/A,N/A,2021-04-09 08:43:06
local,10.110.230.248,CDOMLOTG2E25,Sources,CDB,,N/A,1.51,RUNNING,enabled,CDOMLOTG2E25,N/A,2021-04-09 08:45:26
local,10.110.247.60,DBOMSR3A85E9,Sources,dSource,,N/A,0.68,RUNNING,enabled,DBOMSR3A85E9,N/A,2021-04-09 07:25:52
local,10.110.230.248,oratest,Sources,VDB,DBOMSR3A85E9,2021-04-09 10:26:47 EDT,0.57,RUNNING,enabled,oratest,2021-04-09 10:26:47 EDT,2021-04-09 08:44:20
local,10.110.230.248,PDBTEST1,Sources,VDB,CDOMLOSRCA1DPDB1,2021-04-09 10:54:15 EDT,0.08,RUNNING,enabled,N/A,2021-04-09 10:54:15 EDT,2021-04-09 08:45:21
local,10.110.230.248,PDBTEST2,Sources,VDB,CDOMLOSRCA1DPDB2,2021-04-09 11:44:14 EDT,0.08,RUNNING,enabled,N/A,2021-04-09 11:44:14 EDT,2021-04-09 08:46:16
local,10.110.230.248,VCDB,Sources,vCDB,CDOMLOSRCA1D,N/A,0.67,RUNNING,enabled,VCDB,N/A,2021-04-09 08:46:16
EOF

script_stdout_is $expected_stdout, "list databades results compare";

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-backup', '.',  "create backup"]);

my $some_file  = File::Spec->catfile( "./backup_metadata_vdb.txt" );
my $other_file = File::Spec->catfile( "./backup_metadata_vdb_orig.txt" );
compare_ok($some_file, $other_file, "backup VDB file looks OK");

my $some_file  = File::Spec->catfile( "./backup_metadata_dsource.txt" );
my $other_file = File::Spec->catfile( "./backup_metadata_dsource_orig.txt" );
compare_ok($some_file, $other_file, "backup dsource file looks OK");


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','PDBTEST2',"list pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Parent snapshot,Used(GB),Status,Enabled,Unique Name,Parent time,VDB creation time
local,10.110.230.248,PDBTEST2,Sources,VDB,CDOMLOSRCA1DPDB2,2021-04-09 11:44:14 EDT,0.08,RUNNING,enabled,N/A,2021-04-09 11:44:14 EDT,2021-04-09 08:46:16
EOF

script_stdout_is $expected_stdout, "list pdbtest2 database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','PDBTEST2','-config', "list config pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Env. name,Database,Group,Type,SourceDB,Repository,DB type,Version,Other
local,marcinoratgt.dlpxdc.co,PDBTEST2,Sources,VDB,CDOMLOSRCA1DPDB2,/u01/app/oracle/product/19.0.0.0/dbhome_1,oracle,19.3.0.0.0,-vcdbinstname VCDB,-vcdbname VCDB -vcdbdbname VCDB -vcdbuniqname VCDB -vcdbgroup "Sources",-mntpoint "/mnt/provision"
EOF

script_stdout_is $expected_stdout, "list config pdbtest2 database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','PDBTEST2','-config','-configtype','d', "list config 2 pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Repository,DB type,Version,Server DB name
local,10.110.230.248,PDBTEST2,Sources,VDB,CDOMLOSRCA1DPDB2,/u01/app/oracle/product/19.0.0.0/dbhome_1,oracle,19.3.0.0.0,PDBTEST2
EOF

script_stdout_is $expected_stdout, "list config 2 pdbtest2 database results compare";

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config', '-configtype', 'x', "list config pdbtest2 database"], {"exit" => 1}, "wrong value for configtype");



#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
