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
use Date::Manip;
use POSIX;

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
  'action=s' => \(my $action),
  'retention=s' => \(my $retention),
  'name=s' => \(my $dbname), 
  'format=s' => \(my $format), 
  'type=s' => \(my $type), 
  'group=s' => \(my $group), 
  'host=s' => \(my $host),
  'timeflow=s' => \($timeflow),
  'startDate=s' => \(my $startDate),
  'endDate=s' => \(my $endDate),
  'snapshotname=s' => \(my $snapshotname),
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'skip' => (\my $skip),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);

if ( (! defined($action) ) || ( ! ( ( lc $action eq 'update') || ( lc $action eq 'delete') ) ) ) {
  print "Option -action not defined or has invalid parameter \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ( lc $action eq 'update') && (!defined($retention)) ) {
  print "Action update require a retention parameter \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1); 
}

if (defined($retention)) {
  if (lc $retention eq 'forever') {
    $retention = -1;
  } elsif (lc $retention eq 'policy') {
    $retention = 0;
  } elsif (! isdigit($retention) ) {
    print "Retention parameter has to be a integer or word 'forever' \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit(1);
  }
} 

if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ! ( defined($type) || defined($group) || defined($host) || defined($dbname) || defined($snapshotname) ) ) {
  print "Filter option for snapshot objects is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit(1); 
}


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

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, $debug);
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

    if (scalar(@{$snaplist}) < 1) {
      next;
    }

    if (!defined ($skip)) {

      print "Snapshots list:\n";

      for my $snapitem ( @{$snaplist}) {
        my $snaptime;
        if ($snapshots->isProvisionable($snapitem)) {
          $snaptime = $snapshots->getStartPointwithzone($snapitem);
        } else {
          $snaptime = "not provisionable";
        }
        print "Group '" . $groups->getName($dbobj->getGroup())  . "' DB name '" . $dbobj->getName() . "' Snapshot time: " . $snaptime . " Snapshot name: " . $snapshots->getSnapshotName($snapitem) . "\n";
      }
      print "\n";
      print "Do you want to modify / delete these snapshots: (y/(n)) - use -skip to skip this confirmation \n";


      my $ok = <STDIN>;
      
      chomp $ok;

      if (($ok eq '') || (lc $ok ne 'y')) {
        print "Exiting.\n";
        exit(1);
      }

    }


    for my $snapitem ( @{$snaplist}) {

      if ( lc $action eq 'update' ) {
        if ($snapshots->setRetention($snapitem, $retention + 0)) {
          print "Problem with setting retention on snapshot " . $snapshots->getSnapshotName($snapitem) . "\n";
          $ret = $ret + 1;
        }
      } elsif ( lc $action eq 'delete' ) {
        if ($snapshots->deleteSnapshot($snapitem)) {
          print "Problem with deleting snapshot " . $snapshots->getSnapshotName($snapitem) . "\n";
          $ret = $ret + 1;
        }
      }
    }


  }


}


exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_ctl_snapshots    [ -engine|d <delphix identifier> | -all ] 
                     -action update | delete
                     [-retention days | policy | forever ]
                     [-snapshotname snapshotname]
                     [-group group_name | -name db_name | -host host_name | -type dsource|vdb ] 
                     [-startDate startDate]
                     [-endDate endDate]
                     [-skip]
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

=item B<-action action>
Action to run for snapshot(s). Allowed values are:

update - to change retention

delete - to delete snapshot

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

=item B<-retention retention>
Set snapshot retention to a number of days. Allowed values:

=over 3

=item B<number> - number of days, ex. 10

=item B<policy> - use Retention policy for snapshot

=item B<forever> - keep snapshot forever

=back  

=item B<-skip>
Skip confirmation of update or deletion

=item B<-timeflow a|c>
Display current fimeflow - c (default value), or display all timeflows

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Set policy retention for all snapshot of database "Oracle dsource"

 dx_ctl_snapshots -d Landshark5 -action update -retention policy -name "Oracle dsource"
 Snapshots list:
 Group 'Sources' DB name 'Oracle dsource' Snapshot time: 2016-10-03 07:13:45 EDT Snapshot name: @2016-10-03T11:13:52.335Z
 Group 'Sources' DB name 'Oracle dsource' Snapshot time: 2016-10-12 08:02:16 EDT Snapshot name: @2016-10-12T12:02:31.027Z

 Do you want to modify / delete these snapshots: (y/(n)) - use -skip to skip this confirmation
 y
 Snapshot @2016-10-03T11:13:52.335Z updated
 Snapshot @2016-10-12T12:02:31.027Z updated

Set policy retention for snapshot created after 2016-10-12 13:30 of database autotest

 dx_ctl_snapshots -d Landshark5 -action update -retention policy -name "test2" -startDate "2016-10-12 13:30"
 Snapshots list:
 Group 'Analytics' DB name 'test2' Snapshot time: 2016-10-12 15:20:09 IST Snapshot name: @2016-10-12T14:20:08.826Z

 Do you want to modify / delete these snapshots: (y/(n)) - use -skip to skip this confirmation
 y
 Snapshot @2016-10-12T14:20:08.826Z updated


Set retention to 7 days for all snapshot of database test2 without confirmation

 dx_ctl_snapshots -d Landshark5 -action update -retention 7 -name "test2" -skip
 Snapshot @2016-10-12T12:21:11.234Z updated
 Snapshot @2016-10-12T14:20:08.826Z updated

=cut



