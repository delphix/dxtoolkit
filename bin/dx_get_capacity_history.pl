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
# Copyright (c) 2017 by Delphix. All rights reserved.
#
# Program Name : dx_get_capacity_history.pl
# Description  : Get database and host information
# Author       : Marcin Przepiorowski
# Created      : 08 Mar 2017 (v2.3.x)
#


use warnings;
use strict;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Capacity_obj;
use Formater;
use Databases;
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

my $resolution = 'd';
my $output_unit = 'G';
my $scope = 'system';


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'scope=s' => \($scope),
  'format=s' => \(my $format),
  'name=s' => \(my $dbname),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'dsource=s' => \(my $dsource),
  'host=s' => \(my $host),
  'st=s' => \(my $st),
  'et=s' => \(my $et),
  'debug:i' => \(my $debug),
  'details' => \(my $details),
  'output_unit:s' => \($output_unit),
  'resolution=s' => \($resolution),
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

if (! defined($st)) {
  # take engine time minus 5 min
  $st = "-7days";
}


if (!((lc $resolution eq 'd') || (lc $resolution eq 'h'))) {
  print "Option resolution can have only value d or h \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if (!((lc $scope eq 'system') || (lc $scope eq 'object'))) {
  print "Option scope can have only value system or object \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


Toolkit_helpers::check_filer_options (undef,$type, $group, $host, $dbname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();


if (lc $scope eq 'system') {
  if (defined($details)) {
    $output->addHeader(
      {'Engine',         30},
      {'Timestamp',      30},
      {Toolkit_helpers::get_unit('dS total',$output_unit) ,  15},
      {Toolkit_helpers::get_unit('dS current',$output_unit) ,15},
      {Toolkit_helpers::get_unit('dS log',$output_unit)     ,15},
      {Toolkit_helpers::get_unit('dS snaps',$output_unit)   ,15},
      {Toolkit_helpers::get_unit('VDB total',$output_unit)  ,15},
      {Toolkit_helpers::get_unit('VDB current',$output_unit) ,15},
      {Toolkit_helpers::get_unit('VDB log',$output_unit)    ,15},
      {Toolkit_helpers::get_unit('VDB snaps',$output_unit)  ,15},
      {Toolkit_helpers::get_unit('Total',$output_unit) ,     15},
      {'Usage [%]',      12}
    );
  } else {
    $output->addHeader(
      {'Engine',         30},
      {'Timestamp',      30},
      {Toolkit_helpers::get_unit('dSource',$output_unit),   12},
      {Toolkit_helpers::get_unit('Virtual',$output_unit),   12},
      {Toolkit_helpers::get_unit('Total',$output_unit),     12},
      {'Usage [%]'     , 12}
    );
  }
} else {
  if (defined($details)) {
    $output->addHeader(
      {'Engine',         30},
      {'Timestamp',      30},
      {'Group',          30},
      {'Name',           30},
      {Toolkit_helpers::get_unit('total',$output_unit),   12},
      {Toolkit_helpers::get_unit('current',$output_unit),   12},
      {Toolkit_helpers::get_unit('logS',$output_unit),   12},
      {Toolkit_helpers::get_unit('snaps',$output_unit),   12},
      {Toolkit_helpers::get_unit('locked snaps',$output_unit),   12},
      {Toolkit_helpers::get_unit('held snaps',$output_unit),   12},
      {Toolkit_helpers::get_unit('policy',$output_unit),   12},
      {Toolkit_helpers::get_unit('manual',$output_unit),   12}
    );
  } else {
    $output->addHeader(
      {'Engine',         30},
      {'Timestamp',      30},
      {'Group',          30},
      {'Name',           30},
      {Toolkit_helpers::get_unit('total',$output_unit),   12},
      {Toolkit_helpers::get_unit('current',$output_unit),   12},
      {Toolkit_helpers::get_unit('logS',$output_unit),   12},
      {Toolkit_helpers::get_unit('snaps',$output_unit),   12}
    );
  }
}

my $ret = 0;

my %reshash = (
  'd' => 86400,
  'h' => 3600
);

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };


  my $st_timestamp;

  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st,$engine_obj))) {
    print "Wrong start time (st) format \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (3);
  }

  my $et_timestamp;

  if (defined($et) && (! defined($et_timestamp = Toolkit_helpers::timestamp($et,$engine_obj)))) {
    print "Wrong end time (et) format \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (3);
  }


  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  # filter implementation

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, $dsource, undef, undef, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }

  my $capacity = new Capacity_obj($engine_obj, $debug);

  if (lc $scope eq 'system') {
    # load objects for current engine
    $capacity->LoadSystemHistory($st_timestamp, $et_timestamp, $reshash{$resolution});
    $capacity->processSystemHistory($output,$details, $output_unit);
  } else {
    for my $db_ref (@{$db_list}) {
      my $db_obj = $databases->getDB($db_ref);
      $capacity->LoadObjectHistory($db_ref, $st_timestamp, $et_timestamp, $reshash{$resolution});
      $capacity->processObjectHistory($output,$details, $output_unit);
    }
  }

}




Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_capacity_history [-engine|d <delphix identifier> | -all ]
                         [-details ]
                         [-st "YYYY-MM-DD [HH24:MI:SS]" ]
                         [-et "YYYY-MM-DD [HH24:MI:SS]" ]
                         [-resolution d|h ]
                         [-output_unit K|M|G|T]
                         [-scope system | object ]
                         [-name database_name ]
                         [-type vdb | dsource ]
                         [-group group_name ]
                         [-dsource dsource_name ]
                         [-host host_name ]
                         [-format csv|json ]
                         [-help|? ]
                         [-debug ]

=head1 DESCRIPTION

Get the information about databases space usage.

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

=head1 OPTIONS

=over 3

=item B<-st>
StartTime (format: YYYY-MM-DD [HH24:MI:SS]). Default is "now-7 days".

=item B<-et>
EndTime (format: YYYY-MM-DD [HH24:MI:SS]). Default is "now"

=item B<-details>
Display breakdown of usage.

=item B<-output_unit K|M|G|T>
Display usage using different unit. By default GB are used
Use K for KiloBytes, G for GigaBytes and M for MegaBytes, T for TeraBytes

=item B<-scope system | object >
Switch to display system capacity history or object capacity history.
Default value is system

=item B<-name database_name >
If scope is set to object, display capacity history of the database_name

=item B<-type vdb | dsource >
If scope is set to object, display capacity history of the objects with db type VDB or dSource

=item B<-group group_name >
If scope is set to object, display capacity history of the objects from group_name

=item B<-dsource dsource_name >
If scope is set to object, display capacity history of the objects with dSource dsource_name

=item B<-host host_name >
If scope is set to object, display capacity history of the objects located on the host host_name

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-resoluton d|h>
Display data in daily or hourly aggregation. Default is daily

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Display a history of Delphix Engine utilization

 dx_get_capacity_history -d Landshark51

 Engine                         Timestamp                      dSource [GB] Virtual [GB] Total [GB]   Usage [%]
 ------------------------------ ------------------------------ ------------ ------------ ------------ ------------
 Landshark51                    2017-03-03 05:59:33 GMT                1.22         0.00         1.22         4.22
 Landshark51                    2017-03-03 07:29:34 GMT                1.22         0.00         1.22         4.22
 Landshark51                    2017-03-06 13:56:58 GMT                1.22         0.00         1.22         4.22
 Landshark51                    2017-03-07 13:53:25 GMT                1.22         0.03         1.25         4.34
 Landshark51                    2017-03-09 09:52:50 GMT                1.22         0.03         1.25         4.34
 Landshark51                    2017-03-09 13:22:50 GMT                1.23         0.05         1.28         4.46

Display a history of Delphix Engine utilization with details

 dx_get_capacity_history -d Landshark51 -details

 Engine                         Timestamp                      dS total [GB]   dS current [GB] dS log [GB]     dS snaps [GB]   VDB total [GB]  VDB current [GB VDB log [GB]    VDB snaps [GB]  Total [GB]      Usage [%]
 ------------------------------ ------------------------------ --------------- --------------- --------------- --------------- --------------- --------------- --------------- --------------- --------------- ------------
 Landshark51                    2017-03-03 05:59:33 GMT                   1.22            1.21            0.00            0.00            0.00            0.00            0.00            0.00            1.22         4.22
 Landshark51                    2017-03-03 07:29:34 GMT                   1.22            1.21            0.00            0.00            0.00            0.00            0.00            0.00            1.22         4.22
 Landshark51                    2017-03-06 13:56:58 GMT                   1.22            1.21            0.00            0.00            0.00            0.00            0.00            0.00            1.22         4.22
 Landshark51                    2017-03-07 13:53:25 GMT                   1.22            1.21            0.00            0.00            0.03            0.03            0.00            0.00            1.25         4.34
 Landshark51                    2017-03-09 09:52:50 GMT                   1.22            1.21            0.00            0.00            0.03            0.03            0.00            0.00            1.25         4.34
 Landshark51                    2017-03-09 13:22:50 GMT                   1.23            1.21            0.00            0.01            0.05            0.03            0.01            0.00            1.28         4.46

Display a history of Delphix Engine utilization of the database oratest

 dx_get_capacity_history -d dxtest -scope object -name oratest -output_unit M
 
 Engine                         Timestamp                      Group                          Name                           total [MB]   current [MB] logS [MB]    snaps [MB]
 ------------------------------ ------------------------------ ------------------------------ ------------------------------ ------------ ------------ ------------ ------------
 dxtest                         2023-10-31 02:43:46 PDT        Analytics                      oratest                            589.91       166.58       147.63       250.62
 dxtest                         2023-11-01 02:33:46 PDT        Analytics                      oratest                            761.77       164.85       233.53       338.27
 dxtest                         2023-11-02 02:33:46 PDT        Analytics                      oratest                            922.44       166.20       306.78       424.38
 dxtest                         2023-11-03 02:33:46 PDT        Analytics                      oratest                           1045.52       166.66       351.34       502.41
 dxtest                         2023-11-06 02:21:50 PST        Analytics                      oratest                            907.87       166.67       213.66       502.42
 dxtest                         2023-11-06 03:11:50 PST        Analytics                      oratest                            907.87       166.67       213.66       502.42




=cut
