# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Copyright (c) 2015,2016 by Delphix. All rights reserved.
#
# Program Name : dx_create_env.pl
# Description  : Create a environment in DE
#
# Author       : Marcin Przepiorowski
# Created      : 03 Apr 2015 (v2.0.0)
#



use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Databases;
use Engine;
use Environment_obj;
use Jobs_obj;
use Host_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'envname=s' => \(my $envname),
  'envtype=s'  => \(my $envtype),
  'host=s'  => \(my $host),
  'toolkitdir=s'  => \(my $toolkitdir),
  'username=s' => \(my $username),
  'authtype=s' => \(my $authtype),
  'password=s' => \(my $password),
  'proxy=s' => \(my $proxy),
  'clustername=s' => \(my $crsname),
  'clusterloc=s' => \(my $crshome),
  'sshport=n' => \(my $sshport),
  'asedbuser=s' => \(my $asedbuser),
  'asedbpass=s' => \(my $asedbpass),
  'debug:n' => \(my $debug),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1, -input=>\*DATA);



pod2usage(-verbose => 2, -man=>1, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);
}

if (defined($proxy) && defined($toolkitdir)) {
  print "Option proxy and toolkitdir are mutually exclusive \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);
}

if ( ! ( defined($envname) && defined($envtype) && defined($host) && (defined($toolkitdir) || defined($proxy)  ) && defined($username) && defined($authtype) ) ) {
  print "Options -envname, -envtype, -host, -toolkitdir, -username and -authtype are required. \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);
}

if ( ! ( ( $authtype eq 'password') || ( $authtype eq 'systemkey') ) )  {
  print "Option -authtype has invalid parameter - $authtype \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);
}

if ( ! ( ( lc $envtype eq 'unix') || ( lc $envtype eq 'windows') || ( lc $envtype eq 'rac') ) )  {
  print "Option -envtype has invalid parameter - $envtype \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);
}

if ((lc $envtype eq 'rac' ) && ((!defined($crsname)) || (!defined($crshome))) ) {
  print "Type RAC required clustername and cluserloc to be defined \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);   
}

if ( defined($asedbuser) xor defined($asedbpass) ) {
  print "Option -asedbuser and -asedbpass are required \n";
  pod2usage(-verbose => 1, -input=>\*DATA);
  exit (1);  
}

my $engine_obj = new Engine ($dever, $debug);

$engine_obj->load_config($config_file);


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  my $proxy_ref;
  if (defined($proxy)) {
    my $hosts = new Host_obj($engine_obj, $debug);
    $proxy_ref = $hosts->getHostByAddr($proxy);
    if (!defined($proxy_ref)) {
      print "Can't find proxy host - skippig \n";
      $ret = $ret + 1;
      next;
    }
  }

  my $jobno;

  my $env = new Environment_obj ($engine_obj);
  $jobno = $env->createEnv($envtype,$envname,$host,$toolkitdir,$username,$authtype,$password, $proxy_ref, $crsname, $crshome, $sshport, $asedbuser, $asedbpass);


  if (defined($jobno)) {

    print "Starting adding environment job - $jobno\n";

    my $job = new Jobs_obj($engine_obj,$jobno, 'true');
    my $jobret = $job->waitForJob();
    if ($jobret eq 'COMPLETED') {
      print "Environment job finished with COMPLETED status.\n";
    } else {
      print "There was a problem with job - $jobno. Job status is $jobret. If there is no error on the screen, try with -debug flag to find a root cause\n";
      $ret = $ret + 1;
    }

  } else {
    print "Provision job wasn't started. If there is no error on the screen, try with -debug flag to find a root cause.\n";
    $ret = $ret + 1;
  }
}

exit $ret;

__DATA__
=head1 SYNOPSIS

 dx_create_env [ -engine|d <delphix identifier> | -all ]  -envname environmentname -envtype unix | windows -host hostname 
                   -toolkitdir toolkit_directory | -proxy proxy
                   -username user_name -authtype password | systemkey [ -password password ] 
                   [-clustername name]
                   [-clusterloc loc]
                   [-sshport port]
                   [-asedbuser user]
                   [-asedbpass password]
                   [ -version ] [ -help ] [ -debug ]


=head1 DESCRIPTION

Add a new environment to Delphix Engine

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-envname environmentname>
Environment name

=item B<-envtype type>
Environment type - windows or unix

=item B<-host hostname>
Host name / IP of server being added to Delphix Engine

=item B<-toolkitdir toolkit_directory>
Location for toolkit directory for Unix/Linux 
or location of Delphix Connector directory for Windows

=item B<-proxy proxy>
Proxy server used to access dSource 

=item B<-username user_name>
Server user name

=item B<-authtype password | systemkey>
Authorization type - password or SSH key

=item B<-password password>
If password is specified as authtype - a user password has to be specified

=item B<-clustername name>
Cluser name (CRS name for RAC)

=item B<-clusterloc loc>
Cluser location (CRS home for RAC)

=item B<-sshport port>
SSH port

=item B<-asedbuser user>
ASE DB user for source detection

=item B<-asedbpass password>
ASE DB password for source detection


=back


=head1 OPTIONS

=over 3


=item B<-help>
Print usage information 

=item B<-debug>
Turn on debugging

=item B<-version>
Display version


=back

=head1 EXAMPLES

Adding Unix/Linux environment without ASE discovery

 dx_create_env -d Landshark43 -envname envtest -envtype unix -host 172.16.180.190 -username delphix -authtype password -password xxxxxx -toolkitdir "/u01/app/toolkit"
 Starting adding environment job - JOB-785
 0 - 6 - 8 - 12 - 100
 Job JOB-785 finised with state: COMPLETED Environment job finished with COMPLETED status.

Adding Unix/Linux environment with ASE discovery
 
 dx_create_env -d Landshark5 -envname LINUXSOURCE -envtype unix -host linuxsource -username "delphix" -authtype password -password xxxxx -toolkitdir "/u01/app/toolkit" -asedbuser sa -asedbpass ChangeMeDB
 Starting adding environment job - JOB-1023
 0 - 6 - 8 - 12 - 100
 Job JOB-1023 finised with state: COMPLETED Environment job finished with COMPLETED status.

Adding Unix/Linux environment with Oracle RAC
 
 dx_create_env -d Landshark5 -envname racattack-cl -envtype rac -host 172.16.180.61 -username "oracle" -authtype password -password xxxxxx -toolkitdir "/home/oracle/toolkit" -clusterloc /u01/app/12.1.0.2/grid/ -clustername racattack-cl
 Starting adding environment job - JOB-1024
 0 - 6 - 8 - 12 - 100
 Job JOB-1024 finised with state: COMPLETED Environment job finished with COMPLETED status.


Adding Windows target environment

 dx_create_env -d Landshark5 -envname WINDOWSTARGET -envtype windows -host 172.16.180.132 -username "DELPHIX\delphix_admin" -authtype password -password xxxxxxx -toolkitdir "C:\Program Files\Delphix\DelphixConnector"
 Starting adding environment job - JOB-7495
 0 - 2 - 7 - 8 - 10 - 20 - 60 - 64 - 100
 Job JOB-7495 finished with state: COMPLETED
 Environment job finished with COMPLETED status.

Adding Windows source environment

 dx_create_env -d Landshark5 -envname WINDOWSTARGET -envtype windows -host 172.16.180.132 -username "DELPHIX\delphix_admin" -authtype password -password xxxxxxx -proxy 172.16.180.132
 Starting adding environment job - JOB-7496
 0 - 2 - 7 - 8 - 10 - 20 - 60 - 64 - 100
 Job JOB-7496 finished with state: COMPLETED
 Environment job finished with COMPLETED status.

=cut
