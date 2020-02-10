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
$server->set_dir('landshark');
$server->host('127.0.0.1');
$server->background();

my $server1 = server->new(8082);
$server1->set_dir('Delphix32');
$server1->host('127.0.0.1');
$server1->background();

my $server2 = server->new(8083);
$server2->set_dir('Delphix33');
$server2->host('127.0.0.1');
$server2->background();



# $server->set_dir('dupazbita');
# my $ala = $server->get_dir();
#
# print Dumper $ala;
#
# exit(1);

script_compiles('../../bin/dx_get_hierarchy.pl');

script_runs(['../../bin/dx_get_hierarchy.pl', '-d', 'local', '-format','csv','-nohead'] ,  "All hierachy test");

my $expected_stdout = <<EOF;
local,autofs,Analytics,VDB,TESTEBI,2017-04-06 13:16:37 IST,TESTEBI,autofs
local,autotest,Analytics,VDB,Sybase dsource,2017-05-05 14:43:00 EDT,pubs3,autotest
local,mstest_lsn,Analytics,VDB,AdventureWorksLT2008R2,78000000037201000,AdventureWorksLT2008R2,mstest_lsn
local,mstest_time,Analytics,VDB,AdventureWorksLT2008R2,2017-04-24 06:28:02 PDT,AdventureWorksLT2008R2,mstest_time
local,si4rac,Analytics,VDB,racdba,2016-12-23 13:23:44 UTC,racdba,si4rac
local,siclone,Analytics,VDB,racdba,2016-12-23 13:23:44 UTC,racdba,si4rac
local,targetcon,Analytics,CDB,,N/A,targetcon,N/A
local,vPDBtest,Analytics,VDB,PDB,2017-05-22 11:17:44 EDT,PDB,vPDBtest
local,AdventureWorksLT2008R2,Sources,dSource,,N/A,AdventureWorksLT2008R2,N/A
local,Oracle dsource,Sources,dSource,,N/A,orcl,N/A
local,PDB,Sources,dSource,,N/A,PDB,N/A
local,racdba,Sources,dSource,,N/A,racdba,N/A
local,singpdb,Sources,CDB,,N/A,singpdb,N/A
local,Sybase dsource,Sources,dSource,,N/A,pubs3,N/A
local,TESTEBI,Sources,dSource,,N/A,TESTEBI,N/A
local,VOracledsource_F0C,Tests,VDB,Oracle dsource,2017-06-06 08:58:12 EDT,orcl,VOracledsource_F0C
EOF

script_stdout_is $expected_stdout, "All hierachy results compare";

script_runs(['../../bin/dx_get_hierarchy.pl', '-d', 'local33', '-format','csv','-nohead','-parent_engine','local32'] ,  "2 engine test");

my $expected_stdout = <<EOF;
local33,racdb\@delphix32-2,Sources\@delphix32-2,dSource,,N/A,racdb,N/A
local33,sybase1mask\@delphix32-7,Test\@delphix32-7,VDB,piorotest,2017-03-08 14:35:22 GMT,piorotest,sybase1mask
local33,maskedms\@delphix32-9,Test\@delphix32-9,VDB,tpcc,2017-03-08 17:35:00 GMT,tpcc,maskedms
local33,mask\@delphix32,Test\@delphix32,VDB,test1,2017-05-30 11:11:27 IST,test1,man
local33,cloneMSmas,Untitled,VDB,parent deleted,N/A - timeflow deleted,N/A,N/A
local33,mask1clone,Untitled,VDB,parent deleted,N/A - timeflow deleted,N/A,N/A
local33,mask1clone2,Untitled,VDB,parent deleted,N/A - timeflow deleted,N/A,N/A
local33,mask1clone3,Untitled,VDB,test1,2017-05-30 11:11:27 IST,test1,man
local33,maskclone,Untitled,VDB,parent deleted,N/A - timeflow deleted,N/A,N/A
local33,mssql2clone,Untitled,VDB,tpcc,2017-03-08 17:35:00 GMT,tpcc,maskedms
local33,sybase1clone,Untitled,VDB,piorotest,2017-03-08 14:35:22 GMT,piorotest,sybase1mask
local33,Vracdb_70C,Untitled,VDB,racdb\@delphix32-2,2016-09-28 14:57:16 UTC,racdb,Vracdb_70C
EOF

script_stdout_is $expected_stdout, "2 engine results compare";


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
