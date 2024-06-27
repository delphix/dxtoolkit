use strict;
use Data::Dumper;
use Test::More tests => 9;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../';
use lib '.';
use server;



my $server1 = server->new(8082);
$server1->set_dir('dxtest');
$server1->host('127.0.0.1');
$server1->background();

my $server2 = server->new(8083);
$server2->set_dir('replica');
$server2->host('127.0.0.1');
$server2->background();


script_compiles('../../bin/dx_get_hierarchy.pl');

script_runs(['../../bin/dx_get_hierarchy.pl', '-d', 'local32', '-format','csv','-nohead'] ,  "All hierachy test");

my $expected_stdout = <<EOF;
local32,CDOMLOTG2E25,Analytics,CDB,,N/A,CDOMLOTG2E25,N/A,
local32,mssqltest,Analytics,VDB,Soda,2021-12-17 05:08:25 PST,Soda,mssqltest,Soda
local32,oramask,Analytics,VDB,DBOMSR3A85E9,2021-12-22 07:53:08 EST,DBOMSR3A85E9,oramask,DBOMSR3A85E9
local32,pdbtest,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,CDOMLOSRCA1DPDB1,pdbtest,CDOMLOSRCA1DPDB1
local32,pdbtest2,Analytics,VDB,CDOMLOSRCA1DPDB1,2021-12-17 05:05:09 EST,CDOMLOSRCA1DPDB1,pdbtest2,CDOMLOSRCA1DPDB1
local32,sybasetest,Analytics,VDB,db_rhel83_160_1,2021-12-17 05:01:49 EST,db_rhel83_160_1,sybasetest,db_rhel83_160_1
local32,vcdbtest,Analytics,vCDB,,N/A,vcdbtest,N/A,
local32,CDOMLOSRCA1D,Sources,CDB,,N/A,CDOMLOSRCA1D,N/A,
local32,CDOMLOSRCA1DPDB1,Sources,dSource,,N/A,CDOMLOSRCA1DPDB1,N/A,
local32,db_rhel83_160_1,Sources,dSource,,N/A,db_rhel83_160_1,N/A,
local32,DBOMSR3A85E9,Sources,dSource,,N/A,DBOMSR3A85E9,N/A,
local32,Macaroon,Sources,dSource,,N/A,Macaroon,N/A,
local32,Soda,Sources,dSource,,N/A,Soda,N/A,
EOF

script_stdout_is $expected_stdout, "All hierachy results compare";

script_runs(['../../bin/dx_get_hierarchy.pl', '-d', 'local33', '-format','csv','-nohead','-parent_engine','local32'] ,  "2 engine test");

my $expected_stdout = <<EOF;
local33,oramask\@ip-10-110-251-90-1,Analytics\@ip-10-110-251-90-1,VDB,DBOMSR3A85E9,2021-12-22 07:53:08 EST,DBOMSR3A85E9,oramask,N/A
local33,clony,Untitled,VDB,DBOMSR3A85E9,2021-12-17 06:30:40 EST,DBOMSR3A85E9,oramask,mask2
local33,mask2,Untitled,VDB,DBOMSR3A85E9,2021-12-17 06:30:40 EST,DBOMSR3A85E9,oramask,oramask\@ip-10-110-251-90-1
local33,maskmask,Untitled,VDB,DBOMSR3A85E9,2021-12-22 07:53:08 EST,DBOMSR3A85E9,oramask,oramask\@ip-10-110-251-90-1
EOF

script_stdout_is $expected_stdout, "2 engine results compare";


script_runs(['../../bin/dx_get_hierarchy.pl', '-d', 'local33', '-format','csv','-nohead'] ,  "target engine test");

my $expected_stdout = <<EOF;
local33,oramask\@ip-10-110-251-90-1,Analytics\@ip-10-110-251-90-1,VDB,dSource on other DE,N/A,N/A,N/A,N/A
local33,clony,Untitled,VDB,dSource on other DE,N/A,N/A,N/A,mask2
local33,mask2,Untitled,VDB,dSource on other DE,N/A,N/A,N/A,oramask\@ip-10-110-251-90-1
local33,maskmask,Untitled,VDB,dSource on other DE,N/A,N/A,N/A,oramask\@ip-10-110-251-90-1
EOF

script_stdout_is $expected_stdout, "target engine results compare";

script_runs(['../../bin/dx_get_hierarchy.pl', '-d', 'local33', '-parent_engine','local32', '-printhierarchy'] ,  "hierarchy engine test");

my $expected_stdout = <<EOF;
local33 : oramask\@ip-10-110-251-90-1 --> oramask --> DBOMSR3A85E9
local33 : clony --> mask2 --> oramask\@ip-10-110-251-90-1 --> oramask --> DBOMSR3A85E9
local33 : mask2 --> oramask\@ip-10-110-251-90-1 --> oramask --> DBOMSR3A85E9
local33 : maskmask --> oramask\@ip-10-110-251-90-1 --> oramask --> DBOMSR3A85E9
EOF

script_stdout_is $expected_stdout, "hierarchy engine results compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8082/stop');
my $response = $ua->request($request);

#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8083/stop');
my $response = $ua->request($request);
