use strict;
use Data::Dumper;
use Test::More tests => 6;
use Test::Script;
use LWP::UserAgent;
use lib '/Users/mprzepiorowski/Documents/oss_dxtoolkit/dxtoolkit/lib/';
use lib '.';
use server;




my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();

 
script_compiles('../../bin/dx_refresh_db.pl');

script_runs(['../../bin/dx_refresh_db.pl', '-d', 'local', '-name','siclone', '-timestamp', '2017-05-25 15:15:15'] ,  "refresh on timestamp");

my $expected_stdout = <<EOF;
Starting job JOB-4059 for database siclone.
100
Job JOB-4059 finished with state: COMPLETED
EOF

script_stdout_is $expected_stdout, "refresh on timestamp results compare";

script_compiles('../../bin/dx_rewind_db.pl');

script_runs(['../../bin/dx_rewind_db.pl', '-d', 'local', '-name','siclone', '-timestamp', '2017-07-05 16:14:39'] ,  "rewind on timestamp");

my $expected_stdout = <<EOF;
Starting job JOB-4064 for database siclone.
100
Job JOB-4064 finished with state: COMPLETED
EOF

script_stdout_is $expected_stdout, "rewind on timestamp results compare";

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
