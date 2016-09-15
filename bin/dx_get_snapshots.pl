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
  'timeloc=s' => \($timeloc),
  'timeflow=s' => \($timeflow),
  'startDate=s' => \(my $startDate),
  'endDate=s' => \(my $endDate),
  'snapshotname=s' => \(my $snapshotname),
  'debug:i' => \(my $debug), 
  'details' => \(my $details),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead)
) or pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);

pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
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
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}



if ( ! ( ( lc $timeloc eq 't') || ( lc $timeloc eq 'l') ) )  {
  print "Option -timeloc has invalid parameter - $timeloc \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}

if (lc $timeloc eq 't') {
  $header = 'time';
} else {
  $header = 'loc';
}

if (defined($details)) {

  $output->addHeader(
      {'Engine',         20},
      {'Group',          20},
      {'Database',       20},
      {'Snapshot name',  30},   
      {'Start ' . $header,      25},
      {'End ' . $header,        25},
      {'Creation time ',        25},
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
        {'Start ' . $header,      25},
        {'End ' . $header,        25}
    );  
  } else {
    $output->addHeader(
        {'Engine',         30},
        {'Group',          20},
        {'Database',       30},
        {'Snapshot name',  30},   
        {'Start ' . $header,      25},
        {'End ' . $header,        25},
        {'Timeflow',   10}
    );   
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

  # filter implementation 

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups);
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


    my $snapshots = new Snapshot_obj($engine_obj, $dbitem, $allsnapshots, $debug, $startDate, $endDate);

    my $snaplist = $snapshots->getSnapshots($snapshotname);
    
    if ( (!defined($snaplist)) || (scalar(@{$snaplist}) < 1) ) {
      if (defined($dbname) || defined($group) || defined($type) || defined($host) ) {
        print "There is no snapshots selected for database " . $dbobj->getName() ." on $engine . Please check filter definitions. \n";
        $ret = $ret + 1;
      }
      next;
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


}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_snapshots.pl [ -engine|d <delphix identifier> | -all ] [ -group group_name | -name db_name | -host host_name | -type dsource|vdb ] 
                     [-timeloc t|l] 
                     [-startDate startDate]
                     [-endDate endDate]
                     [-snapshotname snapshotname]
                     [-format csv|json ]  
                     [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about databases capacity.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

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




=cut



