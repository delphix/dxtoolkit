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


script_compiles('../../bin/dx_ctl_dsource.pl');

script_runs(['../../bin/dx_ctl_dsource.pl', '-d', 'local', '-action','create','-group','Sources','-creategroup','-dsourcename',
             'AdventureWorksLT2008R2','-type','mssql','-sourcename','AdventureWorksLT2008R2','-sourceinst','MSSQLSERVER',
             '-sourceenv','WINDOWSSOURCE','-source_os_user','DELPHIX\delphix_admin','-dbuser','aw','-password','delphixdb','-logsync',
             'no','-stageinst','MSSQLSERVER','-stageenv','WINDOWSTARGET','-stage_os_user','DELPHIX\delphix_admin','-validatedsync','NONE',
             '-backup_dir','\\\\172.16.180.133\\backups'] ,  "add dSource with backup");

my $expected_stdout = <<EOF;
Waiting for all actions to complete. Parent action is ACTION-11690
Action completed with success
EOF

script_stdout_is $expected_stdout, "add dSource with backup results compare";


script_runs(['../../bin/dx_ctl_dsource.pl', '-d', 'local', '-action','create','-group','Sources','-creategroup','-dsourcename','test_simple','-type',
            'mssql','-sourcename','test_simple','-sourceinst','MSSQLSERVER','-sourceenv','WINDOWSSOURCE','-source_os_user','DELPHIX\delphix_admin','-dbuser',
            'sa','-password','delphixdb','-logsync','no','-stageinst','MSSQLSERVER','-stageenv','WINDOWSTARGET','-stage_os_user','DELPHIX\delphix_admin','-delphixmanaged','yes']
            ,  "add dSource managed backup");

my $expected_stdout = <<EOF;
Waiting for all actions to complete. Parent action is ACTION-11696
Action completed with success
EOF

script_stdout_is $expected_stdout, "add dSource managed backupresults compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
