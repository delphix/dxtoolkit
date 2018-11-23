use strict;
use Data::Dumper;
use Test::More tests => 5;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;


sub writetofile {
  my $filename = shift;
  my $content = shift;

  open(my $FD, '>', $filename);
  print $FD $content;
  close($FD);

}


my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();


script_compiles('../../bin/dx_get_replication.pl');


script_runs(['../../bin/dx_get_replication.pl', '-d', 'local', '-format', 'csv'], "dx_get_replication get data");

my $expected_stdout = <<EOF;
#Appliance,Profile name,Replication target,Enable,Last Run,Status,Schedule,Run Time,Next run,Objects
local,delphix-rep,delphix-rep,ENABLED,2017-07-10 12:45:00 EDT,COMPLETED,every 15 min on every hour daily ,00:00:28,2017-07-10 13:00:00,G8
local,delphix-rep,delphix-rep,ENABLED,2017-07-10 12:50:00 EDT,COMPLETED,every 15 min on every hour daily ,00:00:33,2017-07-10 13:05:00,G7
local,G5G6-rep,delphix-rep,ENABLED,2017-07-10 12:40:00 EDT,COMPLETED,every 15 min on every hour daily ,00:02:28,2017-07-10 12:55:00,G6,G5
EOF

script_stdout_is $expected_stdout, "dx_get_replication get data results compare";

script_runs(['../../bin/dx_get_replication.pl', '-d', 'local', '-format', 'csv', '-last'], "dx_get_replication get data - last");

my $expected_stdout = <<EOF;
#Appliance,Profile name,Replication target,Last replication ,Avg throughput MB/s,Transfered size MB
local,delphix-rep,delphix-rep,2017-07-10 12:45:00 EDT,    17.38,    70.74
local,delphix-rep,delphix-rep,2017-07-10 12:50:00 EDT,    10.41,    38.52
local,G5G6-rep,delphix-rep,2017-07-10 12:40:00 EDT,    97.28,  1916.43
EOF

script_stdout_is $expected_stdout, "dx_get_replication get data results compare - last";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
