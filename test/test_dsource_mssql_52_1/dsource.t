use strict;
use Data::Dumper;
use Test::More tests => 5;
use Test::Script;
use LWP::UserAgent;
use lib '/Users/mprzepiorowski/Documents/oss_dxtoolkit/dxtoolkit/lib/';
use lib '/Users/mprzepiorowski/Documents/oss_dxtoolkit/dxtoolkit/test/';
use server;




my $server = server->new(8080);
$server->host('127.0.0.1');
$server->background();

 
script_compiles('../../bin/dx_ctl_dsource.pl');

script_runs(['../../bin/dx_ctl_dsource.pl', '-d', 'local','-action','create','-group','Sources','-creategroup','-dsourcename',
             'tpcc','-type','mssql','-sourcename','tpcc','-sourceinst','KVMTARGET2012',
             '-sourceenv','WIN2012N1STD','-source_os_user','DELPHIX\delphix_admin','-dbuser','delphixdb','-password','delphixdb','-logsync',
             'yes','-stageinst','KVMTARGET2012','-stageenv','WIN2012N1STD','-stage_os_user','DELPHIX\delphix_admin','-validatedsync','TRANSACTION_LOG',
             ] ,  "add dSource with backup");


my $expected_stdout = <<EOF;
Waiting for all actions to complete. Parent action is ACTION-273180
Action completed with success
EOF

script_stdout_is $expected_stdout, "add dSource with backup results compare";


script_runs(['../../bin/dx_ctl_dsource.pl', '-d', 'local', '-action','create','-group','Sources','-creategroup','-dsourcename','simple','-type',
            'mssql','-sourcename','simple','-sourceinst','KVMTARGET2012','-sourceenv','WIN2012N1STD','-source_os_user','DELPHIX\delphix_admin','-dbuser',
            'delphixdb','-password','delphixdb','-logsync','no','-stageinst','KVMTARGET2012','-stageenv','WIN2012N1STD','-stage_os_user','DELPHIX\delphix_admin','-delphixmanaged','yes'] 
            ,  "add dSource managed backup");

my $expected_stdout = <<EOF;
Waiting for all actions to complete. Parent action is ACTION-273200
Action completed with success
EOF

script_stdout_is $expected_stdout, "add dSource managed backupresults compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
