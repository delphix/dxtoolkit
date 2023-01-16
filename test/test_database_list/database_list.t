use strict;
use Data::Dumper;
use Test::More tests => 17;
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

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-timeflowparent',  "list databases"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Parent snapshot,Used [GB],Status,Enabled,Unique Name,Parent time,VDB creation time,VDB refresh time
local,10.110.196.207,CDOMLOTG2E25,Analytics,CDB,,N/A,0.82,RUNNING,enabled,CDOMLOTG2E25,N/A,2021-12-17 02:05:50,N/A
local,marcinmssqltgt.dlpxdc.co,mssqltest,Analytics,VDB,Soda,2021-12-17 05:08:25 PST,0.00,RUNNING,enabled,N/A,2021-12-17 05:08:25 PST,2021-12-17 05:14:08,2021-12-17 05:14:11
local,10.110.196.207,oratest,Analytics,VDB,DBOMSR3A85E9,2021-12-17 05:42:50 EST,0.10,RUNNING,enabled,oratest,2021-12-17 05:42:51 EST,2021-12-17 02:05:00,2021-12-17 02:40:23
local,10.110.196.207,pdbtest,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,0.02,RUNNING,enabled,N/A,2021-12-17 05:05:09 EST,2021-12-17 02:05:44,2021-12-17 02:42:13
local,10.110.196.207,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,0.02,RUNNING,enabled,N/A,2021-12-17 05:05:09 EST,2021-12-17 02:06:16,2021-12-17 02:44:03
local,10.110.200.61,sybasetest,Analytics,VDB,db_rhel83_160_1,2021-12-17 05:07:42 EST,0.00,RUNNING,enabled,N/A,2021-12-17 05:07:42 EST,2021-12-17 02:02:10,2021-12-17 02:02:11
local,10.110.196.207,vcdbtest,Analytics,vCDB,CDOMLOSRCA1D,N/A,0.15,RUNNING,enabled,vcdbtest,N/A,2021-12-17 02:06:16,N/A
local,10.110.199.41,CDOMLOSRCA1D,Sources,CDB,,N/A,0.78,RUNNING,enabled,CDOMLOSRCA1D,N/A,2021-12-17 02:01:08,N/A
local,10.110.199.41,CDOMLOSRCA1DPDB1,Sources,dSource,,N/A,0.19,RUNNING,enabled,N/A,N/A,2021-12-17 02:01:08,N/A
local,10.110.216.30,db_rhel83_160_1,Sources,dSource,,N/A,0.00,RUNNING,enabled,N/A,N/A,2021-12-17 02:01:02,N/A
local,10.110.199.41,DBOMSR3A85E9,Sources,dSource,,N/A,0.60,RUNNING,enabled,DBOMSR3A85E9,N/A,2021-12-17 02:01:08,N/A
local,marcinmssqlsrc.dlpxdc.co,Macaroon,Sources,dSource,,N/A,0.01,RUNNING,enabled,N/A,N/A,2021-12-17 05:09:53,N/A
local,marcinmssqlsrc.dlpxdc.co,Soda,Sources,dSource,,N/A,0.01,RUNNING,enabled,N/A,N/A,2021-12-17 05:06:32,N/A
EOF

script_stdout_is $expected_stdout, "list databades results compare";

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv',  "list databases - realrefresh"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Parent snapshot,Used [GB],Status,Enabled,Unique Name,Parent time,VDB creation time,VDB refresh time
local,10.110.196.207,CDOMLOTG2E25,Analytics,CDB,,N/A,0.82,RUNNING,enabled,CDOMLOTG2E25,N/A,2021-12-17 02:05:50,N/A
local,marcinmssqltgt.dlpxdc.co,mssqltest,Analytics,VDB,Soda,2021-12-17 05:08:25 PST,0.00,RUNNING,enabled,N/A,2021-12-17 05:08:25 PST,2021-12-17 05:14:08,2021-12-17 05:14:11
local,10.110.196.207,oratest,Analytics,VDB,DBOMSR3A85E9,2021-12-17 05:04:11 EST,0.10,RUNNING,enabled,oratest,2021-12-17 05:04:16 EST,2021-12-17 02:05:00,2021-12-17 02:40:23
local,10.110.196.207,pdbtest,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,0.02,RUNNING,enabled,N/A,2021-12-17 05:05:09 EST,2021-12-17 02:05:44,2021-12-17 02:42:13
local,10.110.196.207,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,0.02,RUNNING,enabled,N/A,2021-12-17 05:05:09 EST,2021-12-17 02:06:16,2021-12-17 02:44:03
local,10.110.200.61,sybasetest,Analytics,VDB,db_rhel83_160_1,2021-12-17 05:01:49 EST,0.00,RUNNING,enabled,N/A,2021-12-17 05:01:49 EST,2021-12-17 02:02:10,2021-12-17 02:02:11
local,10.110.196.207,vcdbtest,Analytics,vCDB,CDOMLOSRCA1D,N/A,0.15,RUNNING,enabled,vcdbtest,N/A,2021-12-17 02:06:16,N/A
local,10.110.199.41,CDOMLOSRCA1D,Sources,CDB,,N/A,0.78,RUNNING,enabled,CDOMLOSRCA1D,N/A,2021-12-17 02:01:08,N/A
local,10.110.199.41,CDOMLOSRCA1DPDB1,Sources,dSource,,N/A,0.19,RUNNING,enabled,N/A,N/A,2021-12-17 02:01:08,N/A
local,10.110.216.30,db_rhel83_160_1,Sources,dSource,,N/A,0.00,RUNNING,enabled,N/A,N/A,2021-12-17 02:01:02,N/A
local,10.110.199.41,DBOMSR3A85E9,Sources,dSource,,N/A,0.60,RUNNING,enabled,DBOMSR3A85E9,N/A,2021-12-17 02:01:08,N/A
local,marcinmssqlsrc.dlpxdc.co,Macaroon,Sources,dSource,,N/A,0.01,RUNNING,enabled,N/A,N/A,2021-12-17 05:09:53,N/A
local,marcinmssqlsrc.dlpxdc.co,Soda,Sources,dSource,,N/A,0.01,RUNNING,enabled,N/A,N/A,2021-12-17 05:06:32,N/A
EOF

script_stdout_is $expected_stdout, "list databades results compare - realrefresh";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-backup', '.',  "create backup"]);

my $some_file  = File::Spec->catfile( "./backup_metadata_vdb.txt" );
my $other_file = File::Spec->catfile( "./backup_metadata_vdb_orig.txt" );
compare_ok($some_file, $other_file, "backup VDB file looks OK");

my $some_file  = File::Spec->catfile( "./backup_metadata_dsource.txt" );
my $other_file = File::Spec->catfile( "./backup_metadata_dsource_orig.txt" );
compare_ok($some_file, $other_file, "backup dsource file looks OK");


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2',"list pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Parent snapshot,Used [GB],Status,Enabled,Unique Name,Parent time,VDB creation time,VDB refresh time
local,10.110.196.207,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,0.02,RUNNING,enabled,N/A,2021-12-17 05:05:09 EST,2021-12-17 02:06:16,2021-12-17 02:44:03
EOF

script_stdout_is $expected_stdout, "list pdbtest2 database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-parentlast','l',"list pdbtest2 database - last snapshot"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Last snapshot,Used [GB],Status,Enabled,Unique Name,Parent time,VDB creation time,VDB refresh time
local,10.110.196.207,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 06:30:15 EST,0.02,RUNNING,enabled,N/A,N/A,2021-12-17 02:06:16,2021-12-17 02:44:03
EOF

script_stdout_is $expected_stdout, "list pdbtest2 - last snapshot database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config', "list config pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Env. name,Database,Group,Type,SourceDB,Repository,DB type,Version,Other
local,marcinoratgt.dlpxdc.co,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,/u01/app/oracle/product/19.0.0.0/dbhome_1,oracle,19.3.0.0.0,-vcdbinstname vcdbtest,-vcdbname vcdbtest -vcdbdbname vcdbtest -vcdbuniqname vcdbtest -vcdbgroup "Analytics",-mntpoint "/mnt/provision"
EOF

script_stdout_is $expected_stdout, "list config pdbtest2 database results compare";


script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config','-configtype','d', "list config 2 pdbtest2 database"]);

my $expected_stdout = <<EOF;
#Appliance,Hostname,Database,Group,Type,SourceDB,Repository,DB type,Version,Server DB name
local,10.110.196.207,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,/u01/app/oracle/product/19.0.0.0/dbhome_1,oracle,19.3.0.0.0,pdbtest2
EOF

script_stdout_is $expected_stdout, "list config 2 pdbtest2 database results compare";

script_runs(['../../bin/dx_get_db_env.pl', '-d', 'local', '-format','csv','-name','pdbtest2','-config', '-configtype', 'x', "list config pdbtest2 database"], {"exit" => 1}, "wrong value for configtype");



#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
