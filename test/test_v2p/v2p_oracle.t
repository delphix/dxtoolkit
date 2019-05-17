use strict;
use Data::Dumper;
use Test::More tests => 5;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;


system("cp database.oracle/export.json.req database/");
system("cp database.oracle/export.json database/");


my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();


script_runs(['../../bin/dx_v2p.pl', '-d', 'local', '-type','oracle','-timestamp','LATEST_SNAPSHOT','-sourcename','si4rac','-dbname','si','-environment','LINUXTARGET','-envinst','/u01/app/oracle/12.1.0.2/rachome1','-targetDirectory','/u02/backup/'] ,  "oracle v2p on timestamp");

my $expected_stdout = <<EOF;
Starting job - JOB-5033
100
Job JOB-5033 finished with state: COMPLETED
V2P finished..
EOF

script_stdout_is $expected_stdout, "oracle v2p on timestamp results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
