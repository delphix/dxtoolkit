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
# Program Name : dx_get_snapshots.pl
# Description  : Get database and host information
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
# Modified     : 14 Mar 2015 (v2.0.0) Marcin Przepiorowski
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
use Timeflow_obj;
use Capacity_obj;
use Formater;
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;
my $timeloc = 't';
my $timeflow = 'c';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name=s' => \(my $dbname), 
  'format=s' => \(my $format), 
  'type=s' => \(my $type), 
  'group=s' => \(my $group), 
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'timeloc=s' => \($timeloc),
  'timeflow=s' => \($timeflow),
  'startDate=s' => \(my $startDate),
  'endDate=s' => \(my $endDate),
  'snapshotname=s' => \(my $snapshotname),
  'size:s'    => \(my $size),
  'debug:i' => \(my $debug), 
  'details' => \(my $details),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
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

Toolkit_helpers::check_filer_options (undef,$type, $group, $host, $dbname);

my $ret = 0;

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();

my $header;

if ( ! ( ( lc $timeflow eq 'c') || ( lc $timeflow eq 'a') ) )  {
  print "Option -timeflow has invalid parameter - $timeflow \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}



if ( ! ( ( lc $timeloc eq 't') || ( lc $timeloc eq 'l') ) )  {
  print "Option -timeloc has invalid parameter - $timeloc \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($size) && ( ! ( (lc $size eq 'asc') || (lc $size eq 'desc') || (lc $size eq '') ) ) ) {
  print "Option -size has invalid parameter - $size \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (lc $timeloc eq 't') {
  $header = 'time';
} else {
  $header = 'loc';
}

if (defined($size)) {

  $output->addHeader(
      {'Engine',         30},
      {'Group',          20},
      {'Database',       30},
      {'Snapshot name',  30},
      {'Creation time ', 30},   
      {'Size',           30},
      {'Depended objects', 60}
  ); 
  
} else  {

  if (defined($details)) {

    $output->addHeader(
        {'Engine',         20},
        {'Group',          20},
        {'Database',       20},
        {'Snapshot name',  30},   
        {'Start ' . $header,      30},
        {'End ' . $header,        30},
        {'Creation time ',        30},
        {'Timeflow',   10},
        {'Retention',   8},
        {'Version',     4}
    );  

  } else {

    if ( lc $timeflow eq 'c') {
      $output->addHeader(
          {'Engine',         30},
          {'Group',          20},
          {'Database',       30},
          {'Snapshot name',  30},   
          {'Start ' . $header,      30},
          {'End ' . $header,        30}
      );  
    } else {
      $output->addHeader(
          {'Engine',         30},
          {'Group',          20},
          {'Database',       30},
          {'Snapshot name',  30},   
          {'Start ' . $header,      30},
          {'End ' . $header,        30},
          {'Timeflow',   10}
      );   
    }

  }

}


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);  
  my $snapshots;
  my $timeflows;
    
  if (defined($size)) {
    $snapshots = new Snapshot_obj($engine_obj, undef, undef, $debug, undef, undef);
    $timeflows = Timeflow_obj->new($engine_obj, $debug);    
  }

  # filter implementation 
  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, $dsource, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }


  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);

    my $allsnapshots;
    if (lc $timeflow eq 'c') {
      $allsnapshots = '1';
    }

    
    if (defined($size)) {
      if (snapshot_size($output, $groups, $databases, $dbobj, $engine, $engine_obj, $dbitem, $timeflows, $snapshots, $debug)) {
        $ret = $ret + 1;
      }      
    } else {
      if (snapshot_list($output, $groups, $dbobj, $engine, $engine_obj, $dbitem, $allsnapshots, $debug, $startDate, $endDate)) {
        $ret = $ret + 1;
      }
    }


  }


}

if (defined($size) && ($size ne '')) {
  $output->sortbynumcolumn(5, $size);
}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


# Procedure snapshot_list
# parameters: 
#  output - output object
#  groups - groups object 
#  dbobj  - db object
#  engine - engine name
#  engine_obj - engine object
#  dbitem - db reference
#  allsnapshots - timeflow switch
#  debug - debug flag
#  startDate - start date for snapshots
#  endDate - end date for snapshots
# Load snapshot objects from Delphix Engine

sub snapshot_list {
  my $output = shift;
  my $groups = shift;
  my $dbobj = shift;
  my $engine = shift;
  my $engine_obj = shift;
  my $dbitem = shift;
  my $allsnapshots = shift;
  my $debug = shift;
  my $startDate = shift;
  my $endDate = shift;
  

  my $snapshots = new Snapshot_obj($engine_obj, $dbitem, $allsnapshots, $debug, $startDate, $endDate);
  my $snaplist = $snapshots->getSnapshots($snapshotname);
  
  if ( (!defined($snaplist)) || (scalar(@{$snaplist}) < 1) ) {
    if (defined($dbname) || defined($group) || defined($type) || defined($host) ) {
      print "There is no snapshots selected for database " . $dbobj->getName() ." on $engine . Please check filter definitions. \n";
      return 1;
    }
    return 0;
  }

  for my $snapitem ( @{$snaplist}) {

    my $snapstart;
    my $snapstop;

    if ($snapshots->isProvisionable($snapitem)) {
      if ($timeloc eq 't') {
        $snapstart = $snapshots->getStartPointwithzone($snapitem),
        $snapstop = $snapshots->getEndPointwithzone($snapitem),
      } else {
        $snapstart = $snapshots->getStartPointLocation($snapitem);
        $snapstop = $snapshots->getEndPointLocation($snapitem);
      }
    } else {
      $snapstart = 'not provisionable';
      $snapstop = 'not provisionable';     
    }

    if (defined($details)) {

        my $snaptimeflow ;

        if ($snapshots->getSnapshotTimeflow($snapitem) eq $dbobj->getCurrentTimeflow() ) {
          $snaptimeflow = 'current';
        } else {
          $snaptimeflow = 'old';
        }


        $output->addLine(
          $engine,
          $groups->getName($dbobj->getGroup()),
          $dbobj->getName(),
          $snapshots->getSnapshotName($snapitem),
          $snapstart,
          $snapstop,
          $snapshots->getSnapshotCreationTimeWithTimezone($snapitem),
          $snaptimeflow,
          $snapshots->getSnapshotRetention($snapitem),
          $snapshots->getSnapshotVersion($snapitem),
        );  

    } else {
      if (lc $timeflow eq 'c' ) {
        $output->addLine(
          $engine,
          $groups->getName($dbobj->getGroup()),
          $dbobj->getName(),
          $snapshots->getSnapshotName($snapitem),
          $snapstart,
          $snapstop
        ); 
      } else {
        my $snaptimeflow ;

        if ($snapshots->getSnapshotTimeflow($snapitem) eq $dbobj->getCurrentTimeflow() ) {
          $snaptimeflow = 'current';
        } else {
          $snaptimeflow = 'old';
        }


        $output->addLine(
          $engine,
          $groups->getName($dbobj->getGroup()),
          $dbobj->getName(),
          $snapshots->getSnapshotName($snapitem),
          $snapstart,
          $snapstop,
          $snaptimeflow
        );  
      }
    }

  }
  
}


# Procedure snapshot_size
# parameters: 
#  output - output object
#  groups - groups object 
#  databases - databases object
#  dbobj  - db object
#  engine - engine name
#  engine_obj - engine object
#  dbitem - db reference
#  debug - debug flag

# Load snapshot sizes from Delphix Engine

sub snapshot_size {
  my $output = shift;
  my $groups = shift;
  my $databases = shift;
  my $dbobj = shift;
  my $engine = shift;
  my $engine_obj = shift;
  my $dbitem = shift;
  my $timeflows = shift;
  my $snapshots = shift;
  my $debug = shift;
  
  my $capacity = new Capacity_obj($engine_obj, $debug);
  my $all_snaps = $capacity->LoadSnapshots($dbitem);



  for my $snap (@{$all_snaps}) {


    my $snap_ref = $snap->{snapshot};
    my @ddb_array;
        
    for my $ddb (@{$snap->{descendantVDBs}}) {
      my $ddb_name = $databases->getDB($ddb)->getName();
      my $ddb_group = $groups->getName($databases->getDB($ddb)->getGroup());
      my $timeflow = $databases->getDB($ddb)->getCurrentTimeflow();
      
      my $current;
      
      if ($timeflows->getParentSnapshot($timeflow) eq $snap_ref) {
        $current = 'current tf';
      } else {
        $current = 'previous tf'
      }
      
      push(@ddb_array, $ddb_group . '/' . $ddb_name . '/' . $current );
    } 
    
    my $depend_string = join(';', @ddb_array);
    
    $output->addLine(
      $engine,
      $groups->getName($dbobj->getGroup()),
      $dbobj->getName(),
      $snapshots->getSnapshotName($snap_ref),
      $snapshots->getSnapshotCreationTimeWithTimezone($snap_ref),
      sprintf("%10.5f",$snap->{space}),
      $depend_string
    ); 

  }
    
}

__DATA__

=head1 SYNOPSIS

 dx_get_snapshots    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                     [ -group group_name | -name db_name | -host host_name | -type dsource|vdb | -dsource name ] 
                     [ -timeloc t|l] 
                     [ -startDate startDate]
                     [ -endDate endDate]
                     [ -snapshotname snapshotname]
                     [ -format csv|json ]  
                     [ -help|? ] [ -debug ]
                     
 dx_get_snapshots    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                     -size [ asc | desc ]
                     [ -group group_name | -name db_name | -host host_name | -type dsource|vdb | -dsource name ] 
                     [ -format csv|json ]  
                     [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about databases capacity.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

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

Filter databases using one of the following filters

=over 4

=item B<-group>
Group Name

=item B<-name>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)

=item B<-dsource name>
Name of dSource 

=item B<-startDate startDate>
Display snapshot created after startDate

=item B<-endDate endDate>
Display snapshot created before endDate

=item B<-snapshotname snapshotname>
Display snapshot with particular snapshot name


=back

=head1 OPTIONS

=over 3

=item B<-timeloc t|l>
Display snapshot range using a time stamps or location (ex. SCN)

=item B<-timeflow a|c>
Display current fimeflow - c (default value), or display all timeflows

=item B<-details>
Display more details about snapshot - version and retention time in days

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

List all snapshot from engine

 dx_get_snapshots -d de01loca

 Engine                         Group                Database                       Snapshot name                  Start time                End time
 ------------------------------ -------------------- ------------------------------ ------------------------------ ------------------------- -------------------------
 de01loca                       DB                   DBP                            @2015-12-03T08:51:51.462Z      2015-12-03 03:50:39 EST   2015-12-03 09:50:39 EST
 de01loca                       DB                   DBP                            @2016-05-04T02:02:12.594Z      2016-05-04 04:01:11 MEST  2016-05-04 04:01:11 MEST
 de01loca                       DB                   DBP                            @2016-08-14T07:44:44.679Z      2016-08-14 09:43:48 MEST  2016-08-14 09:43:48 MEST
 de01loca                       DB                   DBP                            @2016-09-07T08:30:29.854Z      2016-09-07 10:25:22 MEST  2016-09-07 10:25:22 MEST
 de01loca                       DB                   VDBA                           @2016-11-23T23:30:34.144Z      2016-11-24 00:30:43 CET   2016-11-24 00:30:47 CET
 de01loca                       DB                   VDBA                           @2016-11-24T23:30:25.975Z      2016-11-25 00:30:47 CET   2016-11-25 00:30:51 CET
 de01loca                       DB                   VDBA                           @2016-11-25T23:30:49.476Z      2016-11-26 00:31:10 CET   2016-11-26 00:31:15 CET
 de01loca                       DB                   VDBA                           @2016-11-26T23:30:32.775Z      2016-11-27 00:30:40 CET   2016-11-27 00:30:48 CET
 de01loca                       DB                   VDBA                           @2016-11-27T23:30:20.903Z      2016-11-28 00:30:24 CET   2016-11-28 00:30:28 CET
 de01loca                       DB                   VDBA                           @2016-11-28T23:30:17.095Z      2016-11-29 00:30:22 CET   2016-11-29 00:30:31 CET
 de01loca                       DB                   VDBA                           @2016-11-29T23:30:15.738Z      2016-11-30 00:30:25 CET   2016-11-30 17:03:23 CET
 de01loca                       DB                   VDBC                           @2016-11-23T23:30:42.307Z      2016-11-24 00:30:49 CET   2016-11-24 00:30:52 CET
 de01loca                       DB                   VDBC                           @2016-11-24T23:30:20.147Z      2016-11-25 00:30:27 CET   2016-11-25 00:30:38 CET
 de01loca                       DB                   VDBC                           @2016-11-25T23:30:55.772Z      2016-11-26 00:31:08 CET   2016-11-26 00:31:12 CET


List all snapshots for database VDBA

 dx_get_snapshots -d de01loca -name VDBA

 Engine                         Group                Database                       Snapshot name                  Start time                End time
 ------------------------------ -------------------- ------------------------------ ------------------------------ ------------------------- -------------------------
 de01loca                       DB                   VDBA                           @2016-11-23T23:30:34.144Z      2016-11-24 00:30:43 CET   2016-11-24 00:30:47 CET
 de01loca                       DB                   VDBA                           @2016-11-24T23:30:25.975Z      2016-11-25 00:30:47 CET   2016-11-25 00:30:51 CET
 de01loca                       DB                   VDBA                           @2016-11-25T23:30:49.476Z      2016-11-26 00:31:10 CET   2016-11-26 00:31:15 CET
 de01loca                       DB                   VDBA                           @2016-11-26T23:30:32.775Z      2016-11-27 00:30:40 CET   2016-11-27 00:30:48 CET
 de01loca                       DB                   VDBA                           @2016-11-27T23:30:20.903Z      2016-11-28 00:30:24 CET   2016-11-28 00:30:28 CET
 de01loca                       DB                   VDBA                           @2016-11-28T23:30:17.095Z      2016-11-29 00:30:22 CET   2016-11-29 00:30:31 CET
 de01loca                       DB                   VDBA                           @2016-11-29T23:30:15.738Z      2016-11-30 00:30:25 CET   2016-11-30 17:03:23 CET
 
List all snapshots for database VDBR from all timeflows with details 

 dx_get_snapshots -d de01loca -name VDBR -details -timeflow a

 Engine               Group                Database             Snapshot name                  Start time                End time                  Creation time             Timeflow   Retentio Vers
 -------------------- -------------------- -------------------- ------------------------------ ------------------------- ------------------------- ------------------------- ---------- -------- ----
 de01loca             DB                   VDBR                 @2016-11-24T08:30:01.679Z      2016-11-24 03:30:02 EST   2016-11-25 03:30:03 EST   2016-11-24 03:30:01 EST   old        Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-25T08:30:02.560Z      2016-11-25 03:30:03 EST   2016-11-26 03:30:01 EST   2016-11-25 03:30:02 EST   old        Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-26T08:30:01.550Z      2016-11-26 03:30:01 EST   2016-11-27 03:30:01 EST   2016-11-26 03:30:01 EST   old        Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-27T08:30:01.564Z      2016-11-27 03:30:01 EST   2016-11-27 03:30:03 EST   2016-11-27 03:30:01 EST   old        Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-28T04:25:35.839Z      2016-11-27 23:25:36 EST   2016-11-27 23:25:36 EST   2016-11-27 23:25:35 EST   current    Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-28T08:30:01.724Z      2016-11-28 03:30:02 EST   2016-11-28 03:30:02 EST   2016-11-28 03:30:01 EST   current    Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-29T08:30:01.690Z      2016-11-29 03:30:02 EST   2016-11-30 01:42:17 EST   2016-11-29 03:30:01 EST   current    Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-30T06:42:17.547Z      2016-11-30 01:42:17 EST   2016-11-30 03:30:02 EST   2016-11-30 01:42:17 EST   current    13       11.2
 de01loca             DB                   VDBR                 @2016-11-30T08:30:01.636Z      2016-11-30 03:30:02 EST   2016-11-30 08:42:24 EST   2016-11-30 03:30:01 EST   current    Policy   11.2
 de01loca             DB                   VDBR                 @2016-11-30T13:42:23.915Z      2016-11-30 08:42:24 EST   2016-11-30 11:14:55 EST   2016-11-30 08:42:23 EST   current    Policy   12.1

=cut



