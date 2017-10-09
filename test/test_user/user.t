use strict;
use Data::Dumper;
use Test::More tests => 9;
use Test::Script;
use LWP::UserAgent;
use lib '../../lib/';
use lib '../../test/test_user';
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

 
script_compiles('../../bin/dx_ctl_users.pl');

my $profile = <<EOF;
#Username,Type,Name,Role
dev,group,Analytics,read
EOF

my $auth = <<EOF;
{"user":"USER-38","target":"GROUP-35","type":"Authorization","role":"ROLE-5"}
EOF

writetofile('./profile_set_1', $profile);
writetofile('authorization.json.req',$auth);


script_runs(['../../bin/dx_ctl_users.pl', '-d', 'local', '-profile', './profile_set_1'] ,  "dx_ctl_users add role");

my $expected_stdout = <<EOF;
Role read for target Analytics set for dev
EOF

script_stdout_is $expected_stdout, "dx_ctl_users add role results compare";

my $profile = <<EOF;
#Username,Type,Name,Role
dev,group,Analytics,none
EOF

writetofile('./profile_set_2', $profile);

script_runs(['../../bin/dx_ctl_users.pl', '-d', 'local', '-profile', './profile_set_2'] ,  "dx_ctl_users remove role");


my $expected_stdout = <<EOF;
Role on target Analytics revoked from dev
EOF

script_stdout_is $expected_stdout, "dx_ctl_users remove role results compare";

script_runs(['../../bin/dx_get_users.pl', '-d', 'local', '-profile', '-format','csv'] ,  "dx_get_users all users");

my $expected_stdout = <<EOF;
#Username,First Name,Last Name,Email,work phone,home phone,mobile phone,Authtype,principal,password,admin_priv,js_user
sysadmin,,,,,,,NATIVE,,password,N,N
delphix_admin,,,marcin\@delphix.com,,,,NATIVE,,password,Y,N
dev,dev,,dev\@test.com,,,,NATIVE,,password,N,N
js,,,js\@test.com,,,,NATIVE,,password,N,Y
user,user,user,user\@site.net,,,,LDAP,"uid=user,ou=People,DC=CA,DC=DOMAIN",,N,N
ala,,,ala\@ma.kota.com,,,,NATIVE,,password,N,N
testuser,Test,User,test.user\@test.com,,555-222-333,,NATIVE,,password,Y,Y
#Username,Type,Name,Role
dev,group,Analytics,Read
dev,group,Analytics,Data
dev,databases,Oracle dsource,PROVISIONER
user,databases,Oracle dsource,PROVISIONER
user,databases,PDB,Data
user,databases,siclone,PROVISIONER
ala,databases,PDB,Data
ala,databases,siclone,PROVISIONER
ala,databases,targetcon,OWNER
ala,databases,targetcon,Data
EOF

script_stdout_is $expected_stdout, "dx_get_users all users results compare";

script_runs(['../../bin/dx_get_users.pl', '-d', 'local', '-profile', '-format','csv','-username','dev'] ,  "dx_get_users all users");

my $expected_stdout = <<EOF;
#Username,First Name,Last Name,Email,work phone,home phone,mobile phone,Authtype,principal,password,admin_priv,js_user
dev,dev,,dev\@test.com,,,,NATIVE,,password,N,N
#Username,Type,Name,Role
dev,group,Analytics,Read
dev,group,Analytics,Data
dev,databases,Oracle dsource,PROVISIONER
EOF

script_stdout_is $expected_stdout, "dx_get_users one user results compare";


#stop server
my $ua = LWP::UserAgent->new;
$ua->agent("Delphix Perl Agent/0.1");
$ua->timeout(15);
my $request = HTTP::Request->new(GET => 'http://127.0.0.1:8080/stop');
my $response = $ua->request($request);
