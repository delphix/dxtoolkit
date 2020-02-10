use strict;
use Data::Dumper;
use Test::More tests => 12;
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


script_compiles('../../bin/dx_get_env.pl');

script_runs(['../../bin/dx_get_env.pl', '-d', 'local', '-format','csv',  "list environments"]);

my $expected_stdout = <<EOF;
#Appliance,Environment Name,Type,Status,OS Version
local,marcinoraclesrc.dcenter.delphix.com,unix,enabled,Red Hat Enterprise Linux Server release 6.5 (Santiago)
local,marcinoracletgt.dcenter.delphix.com,unix,enabled,Red Hat Enterprise Linux Server release 6.5 (Santiago)
EOF

script_stdout_is $expected_stdout, "list environments results compare";

script_runs(['../../bin/dx_get_env.pl', '-d', 'local', '-format','csv','-name','marcinoracletgt.dcenter.delphix.com',  "list one environment"]);

my $expected_stdout = <<EOF;
#Appliance,Environment Name,Type,Status,OS Version
local,marcinoracletgt.dcenter.delphix.com,unix,enabled,Red Hat Enterprise Linux Server release 6.5 (Santiago)
EOF

script_stdout_is $expected_stdout, "list one environment results compare";

script_runs(['../../bin/dx_get_env.pl', '-d', 'local', '-format','csv', '-replist', "list repos"]);

my $expected_stdout = <<EOF;
#Appliance,Environment Name,Repository list
local,marcinoraclesrc.dcenter.delphix.com,
,,Unstructured Files
,,/u01/app/ora12102/product/12.1.0/dbhome_1
,,/u01/app/ora12201/product/12.2.0/dbhome_1
local,marcinoracletgt.dcenter.delphix.com,
,,Unstructured Files
,,/u01/app/ora12102/product/12.1.0/dbhome_1
,,/u01/app/ora12201/product/12.2.0/dbhome_1
EOF

script_stdout_is $expected_stdout, "list repos results compare";

script_runs(['../../bin/dx_get_env.pl', '-d', 'local', '-backup', '.',  "create backup"]);

my $some_file  = File::Spec->catfile( "./backup_env.txt" );
my $other_file = File::Spec->catfile( "./backup_env_orig.txt" );
compare_ok($some_file, $other_file, "backup file looks OK");


script_compiles('../../bin/dx_ctl_env.pl');

script_runs(['../../bin/dx_ctl_env.pl', '-d', 'local', '-name','marcinoracletgt.dcenter.delphix.com','-action','refresh',  "refresh environment"]);

my $expected_stdout = <<EOF;
Refreshing environment marcinoracletgt.dcenter.delphix.com
Starting job JOB-7 for environment marcinoracletgt.dcenter.delphix.com.
100
Job JOB-7 finished with state: COMPLETED
EOF

script_stdout_like "Job JOB-7 finished with state: COMPLETED", "refresh environment results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
