use strict;
use Data::Dumper;
use Test::More tests => 3;
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


script_compiles('../../bin/dx_provision_vdb.pl');

script_runs(['../../bin/dx_provision_vdb.pl', '-d', 'local', '-type', 'oracle', '-group', 'Analytics', '-creategroup',
             '-sourcename','PDBX1', '-srcgroup', 'Sources', '-targetname', 'pdbtest2', '-dbname', 'PDBTEST2',
             '-environment','marcinoracletgt.dcenter.delphix.com', '-envinst', '/u01/app/ora12102/product/12.1.0/dbhome_1',
             '-envUser', 'ora12102', '-vcdbname', 'vcdbtest', '-vcdbdbname', 'VCDBTEST', '-vcdbinstname', 'vcdbtest', 
             '-vcdbuniqname', 'vcdbtest', '-vcdbgroup', 'Analytics', '-mntpoint', '/mnt/provision'],  "Provision vdb");

my $expected_stdout = <<EOF;
Starting job - JOB-59
100
Job JOB-59 finished with state: COMPLETED
VDB created..
EOF

script_stdout_like "Job JOB-59 finished with state: COMPLETED", "provision vpdb results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
