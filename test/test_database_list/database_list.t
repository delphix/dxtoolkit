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
local,10.43.6.187,cdbkate,Analytics,CDB,,N/A,0.86,UNKNOWN,disabled,cdbkate,N/A,2019-12-30 03:26:13
local,10.43.6.187,oratest,Analytics,VDB,marina,2019-12-30 03:20:58 PST,0.01,RUNNING,enabled,oratest,N/A,2019-12-30 03:21:37
local,10.43.6.187,pdbtest,Analytics,VDB,PDBX1,2019-12-30 03:21:29 PST,0.00,UNKNOWN,disabled,N/A,N/A,2019-12-30 03:25:45
local,10.43.6.187,pdbtest2,Analytics,VDB,PDBX1,2019-12-30 03:31:23 PST,0.00,RUNNING,enabled,N/A,N/A,2019-12-30 03:36:52
local,10.43.6.187,vcdbtest,Analytics,vCDB,carmel,N/A,0.02,RUNNING,enabled,vcdbtest,N/A,2019-12-30 03:36:52
local,10.43.1.211,carmel,Sources,CDB,,N/A,0.90,UNKNOWN,disabled,carmel,N/A,2019-12-30 03:13:21
local,10.43.1.211,marina,Sources,dSource,,N/A,0.60,RUNNING,enabled,marina,N/A,2019-12-30 03:09:05
local,NA,PDBX1,Sources,detached,,N/A,0.27,NA,N/A,N/A,N/A,2019-12-30 03:13:21
EOF

script_stdout_is $expected_stdout, "list databades results compare";

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-backup', '.',  "create backup"]);

my $some_file  = File::Spec->catfile( "./backup_metadata_vdb.txt" );
my $other_file = File::Spec->catfile( "./backup_metadata_vdb_orig.txt" );
compare_ok($some_file, $other_file, "backup VDB file looks OK");

my $some_file  = File::Spec->catfile( "./backup_metadata_dsource.txt" );
my $other_file = File::Spec->catfile( "./backup_metadata_dsource_orig.txt" );
compare_ok($some_file, $other_file, "backup dsource file looks OK");


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2',"list pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Parent snapshot,Used(GB),Status,Enabled,Unique Name,Parent time,VDB creation time
local,10.43.6.187,pdbtest2,Analytics,VDB,PDBX1,2019-12-30 03:31:23 PST,0.00,RUNNING,enabled,N/A,N/A,2019-12-30 03:36:52
EOF

script_stdout_is $expected_stdout, "list pdbtest2 database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config', "list config pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Env. name,Database,Group,Type,SourceDB,Repository,DB type,Version,Other
local,marcinoracletgt.dcenter.delphix.com,pdbtest2,Analytics,VDB,PDBX1,/u01/app/ora12102/product/12.1.0/dbhome_1,oracle,12.1.0.2.0,-vcdbname vcdbtest -vcdbdbname VCDBTEST -vcdbinstname vcdbtest -vcdbuniqname vcdbtest -vcdbgroup Analytics,-mntpoint "/mnt/provision"
EOF

script_stdout_is $expected_stdout, "list config pdbtest2 database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config','-configtype','d', "list config 2 pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Repository,DB type,Version,Server DB name
local,10.43.6.187,pdbtest2,Analytics,VDB,PDBX1,/u01/app/ora12102/product/12.1.0/dbhome_1,oracle,12.1.0.2.0,PDBTEST2
EOF

script_stdout_is $expected_stdout, "list config 2 pdbtest2 database results compare";

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config', '-configtype', 'x', "list config pdbtest2 database"], {"exit" => 1}, "wrong value for configtype");



#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
