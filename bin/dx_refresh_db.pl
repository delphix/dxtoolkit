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
# Program Name : dx_refresh_db.pl
# Description  : Control VDB and dsource databases
# Author       : Marcin Przepiorowski
# Created      : 12 May 2015 (v2.0.0)
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
use Jobs_obj;
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name=s' => \(my $dbname),  
  'type=s' => \(my $type), 
  'group=s' => \(my $group), 
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'timestamp=s' => \(my $timestamp),
  'location=s' => \(my $changenum),
  'debug:n' => \(my $debug), 
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'parallel=n' => \(my $parallel),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);


pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   


my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($timestamp) && defined($changenum)) {
  print "Parameter timestamp and location are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (! defined($timestamp)) {
  $timestamp = 'LATEST_SNAPSHOT';
}

Toolkit_helpers::check_filer_options (1,$type, $group, $host, $dbname, undef, $dsource);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);  

  # filter implementation 
  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, $dsource, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  my @jobs;

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {

    my $dbobj = $databases->getDB($dbitem);
    my $dbname = $dbobj->getName();
    my $jobno;

    if ($dbobj->getType() ne 'VDB') {
      print "You can't refresh dSource $dbname \n";
      $ret = $ret + 1;
      next;
    }

    if (defined($changenum)) {
      undef $timestamp; 
    }


    $jobno = $dbobj->refresh($timestamp, $changenum);

    if (defined ($jobno) ) {
      print "Starting job $jobno for database $dbname.\n";
      my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);
      if (defined($parallel)) {
        push(@jobs, $job);
      } else {
        my $jobstat = $job->waitForJob();
        if ($jobstat ne 'COMPLETED') {
          $ret = $ret + 1;
        }
      }
    } else {
      print "Problem with defining refresh job\n";
      $ret = $ret + 1;
    }

    if (defined($parallel)) {

      if ((scalar(@jobs) >= $parallel ) || (scalar(@{$db_list}) eq scalar(@jobs) )) {
        my $pret = Toolkit_helpers::parallel_job(\@jobs);
        $ret = $ret + $pret;
      }
    }

  }
  
  if (defined($parallel) && (scalar(@jobs) > 0)) {
    while (scalar(@jobs) > 0) {
      my $pret = Toolkit_helpers::parallel_job(\@jobs);
      $ret = $ret + $pret; 
    }   
  }


}

exit $ret;


__DATA__


=head1 SYNOPSIS

 dx_refresh_db  [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                < -group group_name | -name db_name | -host host_name | -type dsource|vdb > 
                [ -timestamp timestamp] 
                [ -help|? ] 
                [ -debug ] 
                [ -parallel p]

=head1 DESCRIPTION

Refresh database(s) selected by filter on selected engine(s) to the specified point in time.

=head1 ARGUMENTS

=head2 Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head2 Filters

=over 4

=item B<-group>
Group Name

=item B<-name>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)

=item B<-dsource dsourcename>
Dsource name

=back 

=head1 OPTIONS

=over 3

=item B<-timestamp>
Time stamp for export format (YYYY-MM-DD HH24:MI:SS in VBD timezone) or LATEST_POINT or LATEST_SNAPSHOT or bookmark name
Default is LATEST_SNAPSHOT

=item B<-location>
Point in time defined by SCN for Oracle and LSN for MS SQL 

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-parallel maxjob>
Run action on all targets in parallel. Limit number of jobs to maxjob.

=back

=head1 EXAMPLES

Refresh one VDB using latest snapshot from dSource

 dx_refresh_db -d Landshark -name autoprov
 Starting job JOB-241 for database autoprov. 
 0 - 10 - 25 - 29 - 31 - 33 - 36 - 40 - 49 - 53 - 55 - 58 - 59 - 60 - 61 - 62 - 63 - 70 - 100
 Job JOB-241 finised with state: COMPLETED

Refresh one VDB using snapshot taken on 2015-10-12 7:56 on dSource

  dx_refresh_db -d Landshark43 -name testsybase -timestamp '2015-10-12 07:56'
  Starting job JOB-822 for database testsybase.
  0 - 2 - 6 - 32 - 36 - 40 - 88 - 100
  Job JOB-822 finised with state: COMPLETED

Refresh all VDBs provisioned from dSource racdb using bookmark on dSource

   dx_refresh_db -d Delphix32 -dsource racdb -timestamp test_bookmark
   Starting job JOB-267187 for database Vrac_08B.
   0 - 12 - 25 - 31 - 34 - 35 - 40 - 46 - 58 - 64 - 67 - 70 - 71 - 72 - 73 - 74 - 76 - 85 - 100
   Job JOB-267187 finished with state: COMPLETED
   Starting job JOB-267193 for database Vracdb_FEE.
   0 - 12 - 25 - 31 - 34 - 35 - 40 - 58 - 64 - 67 - 70 - 71 - 72 - 73 - 74 - 76 - 85 - 100
   Job JOB-267193 finished with state: COMPLETED




=cut



