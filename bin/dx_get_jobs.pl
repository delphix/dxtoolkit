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
# Copyright (c) 2014,2016 by Delphix. All rights reserved.
#
# Program Name : dx_get_jobs.pl
# Description  : Get Delphix Engine audit
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
# Modified     : 31 Aug 2015 (v2.0.0) Marcin Przepiorowski
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
use Engine;
use Formater;
use Toolkit_helpers;
use Jobs;
use User_obj;
use Users;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'state=s' => \(my $state),
  'jobref=s'   => \(my $jobref),
  'dbname=s' => \(my $dbname),  
  'type=s' => \(my $type), 
  'group=s' => \(my $group), 
  'host=s' => \(my $host),
  'outdir=s' => \(my $outdir),
  'dsource=s' => \(my $dsource),
  'format=s' => \(my $format), 
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}



if (defined($state) && ( ! ( (uc $state eq 'COMPLETED') || (uc $state eq 'FAILED') || (uc $state eq 'RUNNING') || (uc $state eq 'SUSPENDED') || (uc $state eq 'CANCELED')  ) ) ) {
  print "Option state can have only COMPLETED, WAITING and FAILED value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


$output->addHeader(
    {'Appliance',   20},
    {'Job ref  ',   15},   
    {'Target name', 20},
    {'Username',    20}, 
    {'Start date',  30},
    {'End date',    30},
    {'Run time',    10},
    {'State',       12},
    {'Type',        20}
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };
  
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);  
  my $db_list;
  my %db_map;
  
  for my $dbitem ( $databases->getDBList() ) {
     
     my $dbobj = $databases->getDB($dbitem);
     my $dbname = $dbobj->getName();
     my $groupname = $groups->getName($dbobj->getGroup());
     
     $db_map { $dbitem } = $groupname . '/' . $dbname;
     if ( defined( $dbobj->{source}->{reference} ) ) {
        $db_map { $dbobj->{source}->{reference} } = $groupname . '/' . $dbname;
     }
     
     if ( defined($dbobj->{staging_source} ) ) {
        $db_map { $dbobj->{staging_source}->{reference} } = 'Staging - '. $dbname;
     }
  }
  
  
  if (defined($dbname) || defined($host) || defined($group) || defined($type) || defined($dsource) ) {
     $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, $dsource, undef, undef, $debug);
     
     if (!defined($db_list)) {
        print "Object not found. Skipping jobs\n";
        next;
     }
     
  }
  
  my $st_timestamp;

  if (! defined($st)) {
      # take engine time minus 1 day
    $st = $engine_obj->getTime(7*24*60);
    $st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj);
  } else {
    if (! defined($st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj))) {
      print "Wrong start time (st) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);  
    }
  }

  my $et_timestamp;

  if (defined($et)) {
    $et = Toolkit_helpers::timestamp_to_timestamp_with_de_timezone($et, $engine_obj);
    if (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj))) {
      print "Wrong end time (et) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);  
    } 
  }


  my $jobs = new Jobs($engine_obj, $st_timestamp, $et_timestamp, $state, undef, undef, $jobref, $db_list,  $debug);  
  my $users = new Users($engine_obj, $databases, $debug);

  my @jobsarr;
  @jobsarr = @{$jobs->getJobList('asc')};


  for my $jobitem ( @jobsarr ) {

    my $jobobj = $jobs->getJob($jobitem);

    my $username;
    #print Dumper $users->getUser()->getName();
    if (defined(my $userref = $jobobj->getUser())) {
      if (defined($users->getUser($userref))) {
         $username = $users->getUser($userref)->getName();
      } else {
         $username = 'N/A';
      }
    } else {
      $username = 'N/A';
    }
    
    my $target_ref = $jobobj->getJobTarget();
    my $target_name;
    
    if (defined($db_map{$target_ref})) {
      $target_name = $db_map{$target_ref};
   } else {
      $target_name = $jobobj->getJobTargetName();
   }

    $output->addLine(
      $engine,
      $jobitem,
      $target_name,
      $username,
      $jobobj->getJobStartTimeWithTZ(),
      $jobobj->getJobUpdateTimeWithTZ(),
      $jobobj->getJobRuntime(),
      $jobobj->getJobState(),
      $jobobj->getJobActionType()
    )

  }
}

if (defined($outdir)) {
  Toolkit_helpers::write_to_dir($output, $format, $nohead,'jobs',$outdir,1);
} else {
  Toolkit_helpers::print_output($output, $format, $nohead);
}


__DATA__

=head1 SYNOPSIS

 dx_get_jobs.pl [ -engine|d <delphix identifier> | -all ] 
                [-jobref ref] 
                [-st timestamp] 
                [-et timestamp] 
                [-dbname name]
                [-group group]
                [-host hostname]
                [-type VDB|dSource]
                [-dSource name]
                [-state state] 
                [-format csv|json ]  
                [-outdir path]
                [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of jobs from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

Filter faults using one of the following filters

=over 4

=item B<-state>
Job state - COMPLETED / FAILED / RUNNING / SUSPENDED / CANCELED

=item B<-jobref ref>
Job reference

=back

=head1 OPTIONS

=over 3

=item B<-st timestamp>
Start time for faults list - default value is 7 days

=item B<-et timestamp>
End time for faults list 

=item B<-group group>
Database group Name

=item B<-dbname name>
Database Name

=item B<-host host>
Database host Name

=item B<-type type>
Type (dsource|vdb)

=item B<-dsource name>
Name of dSource

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-outdir path>                                                                                                                                            
Write output into a directory specified by path.
Files names will include a timestamp and type name

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Print jobs from Delphix Engine

 dx_get_jobs -d Landshark5

 Appliance            Job ref         Target name          Username             Start date                     End date                       Run time   State        Type
 -------------------- --------------- -------------------- -------------------- ------------------------------ ------------------------------ ---------- ------------ --------------------
 Landshark5           JOB-7753        Test/vFiles          N/A                  2016-11-01 17:20:10 GMT        2016-11-01 17:20:11 GMT        00:00:01   COMPLETED    DB_SYNC
 Landshark5           JOB-7754        Test/vFiles          N/A                  2016-11-02 08:27:37 GMT        2016-11-02 08:27:37 GMT        00:00:00   COMPLETED    DB_SYNC
 Landshark5           JOB-7755        Analytics/VOra_744   delphix_admin        2016-11-08 12:28:52 GMT        2016-11-08 12:29:19 GMT        00:00:27   CANCELED     DB_PROVISION
 Landshark5           JOB-7756        VOra_744             delphix_admin        2016-11-08 12:29:03 GMT        2016-11-08 12:29:19 GMT        00:00:16   COMPLETED    SOURCE_DISABLE
 Landshark5           JOB-7757        VOra_744             delphix_admin        2016-11-08 12:29:10 GMT        2016-11-08 12:29:19 GMT        00:00:09   COMPLETED    SOURCE_STOP
 Landshark5           JOB-7758        Analytics/VOra_744   delphix_admin        2016-11-08 12:32:21 GMT        2016-11-08 12:32:22 GMT        00:00:01   COMPLETED    DB_DELETE

Print jobs for database "cont1" since 2016-10-01

 dx_get_jobs -d Landshark5 -dbname cont1 -st "2016-10-01"

 Appliance            Job ref         Target name          Username             Start date                     End date                       Run time   State        Type
 -------------------- --------------- -------------------- -------------------- ------------------------------ ------------------------------ ---------- ------------ --------------------
 Landshark5           JOB-7694        Analytics/cont1      delphix_admin        2016-10-24 19:13:47 IST        2016-10-24 19:15:21 IST        00:01:34   COMPLETED    DB_PROVISION
 Landshark5           JOB-7696        Analytics/cont1      delphix_admin        2016-10-24 19:15:18 IST        2016-10-24 19:15:21 IST        00:00:03   COMPLETED    DB_SYNC
 Landshark5           JOB-7700        Analytics/cont1      delphix_admin        2016-10-24 19:21:20 IST        2016-10-24 19:21:21 IST        00:00:01   COMPLETED    DB_SYNC
 Landshark5           JOB-7702        Analytics/cont1      delphix_admin        2016-10-24 19:21:25 IST        2016-10-24 19:22:55 IST        00:01:30   COMPLETED    DB_REFRESH
 Landshark5           JOB-7710        Analytics/cont1      delphix_admin        2016-10-24 19:22:56 IST        2016-10-24 19:22:59 IST        00:00:03   COMPLETED    DB_SYNC
 Landshark5           JOB-7712        Analytics/cont1      N/A                  2016-10-25 08:30:00 IST        2016-10-25 08:30:01 IST        00:00:01   COMPLETED    DB_SYNC
 Landshark5           JOB-7717        Analytics/cont1      dev                  2016-10-25 08:38:01 IST        2016-10-25 08:38:02 IST        00:00:01   COMPLETED    DB_SYNC
 Landshark5           JOB-7719        Analytics/cont1      dev                  2016-10-25 08:38:06 IST        2016-10-25 08:39:36 IST        00:01:30   COMPLETED    DB_REFRESH
 Landshark5           JOB-7721        Analytics/cont1      dev                  2016-10-25 08:39:36 IST        2016-10-25 08:39:39 IST        00:00:03   COMPLETED    DB_SYNC
 Landshark5           JOB-7723        Analytics/cont1      dev                  2016-10-25 08:41:33 IST        2016-10-25 08:41:34 IST        00:00:01   COMPLETED    DB_SYNC
 Landshark5           JOB-7725        Analytics/cont1      dev                  2016-10-25 08:42:11 IST        2016-10-25 08:42:12 IST        00:00:01   COMPLETED    DB_SYNC

Print jobs for all VDB based on dSource "Oracle dsource"

 dx_get_jobs -d Landshark5 -dsource "Oracle dsource"

 Appliance            Job ref         Target name          Username             Start date                     End date                       Run time   State        Type
 -------------------- --------------- -------------------- -------------------- ------------------------------ ------------------------------ ---------- ------------ --------------------
 Landshark5           JOB-7795        Analytics/test       delphix_admin        2016-11-08 13:11:45 GMT        2016-11-08 13:13:03 GMT        00:01:18   COMPLETED    DB_PROVISION
 Landshark5           JOB-7796        Analytics/test       delphix_admin        2016-11-08 13:13:00 GMT        2016-11-08 13:13:03 GMT        00:00:03   COMPLETED    DB_SYNC

=cut



