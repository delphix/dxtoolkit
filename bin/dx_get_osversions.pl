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
# Copyright (c) 2016 by Delphix. All rights reserved.

# Program Name : dx_get_osversions.pl
# Description  : Get installed OS versions
# Author       : Marcin Przepiorowski
# Created      : April 2019
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use File::Spec;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use Version_obj;


my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'format=s' => \(my $format),
  'steps' => \(my $steps),
  'report' => \(my $report),
  'target=s' => \(my $target),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'nohead' => \(my $nohead),
  'version' => \(my $print_version),
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


if (defined($steps) && defined($report)) {
   print "Options -report and -steps are mutually exclusive \n";
   pod2usage(-verbose => 1,  -input=>\*DATA);
   exit (1);
}

my $output = new Formater();

if (defined($steps)) {
  $output->addHeader(
      {'engine name',          35},
      {'from',                 15},
      {'to',                   15},
      {'step',                 30},
      {'status',               15},
      {'severity',             15},
      {'Start test date',      30}
  );
} elsif (defined($report)) {
  $output->addHeader(
      {'engine name',          35},
      {'to',                   15},
      {'title',                30},
      {'status',               15},
      {'severity',             15}
  );
} else {
  $output->addHeader(
      {'engine name',          35},
      {'name',                 15},
      {'status',               30},
      {'install date',         30}
  );
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
  print "Can't connect to Dephix Engine $dx_host\n\n";
  $ret = $ret + 1;
  next;
  };

  if ($engine_obj->getCurrentUserType() ne 'SYSTEM') {
   print "User with sysadmin role is required for this script to run. Please check dxtools.conf entry for $engine\n";
   next;
  }


  my $version_obj = new Version_obj($engine_obj, $debug);
  if (defined($steps)) {
    $version_obj->loadVerfication();

    for my $repref (@{$version_obj->getReportList()}) {
        my ($running_version, $verified_version) = $version_obj->getReportVersions($repref);
        if (defined($target))  {
          if ($target ne $verified_version) {
            $ret = 1;
            next;
          } else {
            # target found
            $ret = 0;
          }
        } 
        $output->addLine(
            $engine,
            $running_version,
            $verified_version,
            '',
            '',
            '',
            ''
        );
        $version_obj->getReportSteps($repref, $output);
    }
  } elsif ( defined($report) ) {
    $version_obj->loadVerfication();

    for my $oshash (@{$version_obj->getOSversions()}) {
      my $tover = $version_obj->getOSName($oshash);
      if (defined($target))  {
        if ($target ne $tover) {
          $ret = 1;
          next;
        } else {
          # target found
          $ret = 0;
        }
      } 
      $output->addLine(
          $engine,
          $tover,
          '',
          '',
          ''
      );
      $version_obj->loadUpgradeReport($oshash);
      $version_obj->getReportResults($oshash, $output)
    }

  } else {
    for my $oshash (@{$version_obj->getOSversions()}) {
      $output->addLine(
            $engine,
            $version_obj->getOSName($oshash),
            $version_obj->getOSStatus($oshash),
            $version_obj->getInstalTime($oshash)
      );
    }
  }





}


Toolkit_helpers::print_output($output, $format, $nohead);


exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_osversions    [-engine|d <delphix identifier> | -all ]
                      [-steps | -report ]
                      [-target targetversion]
                      [-format csv|json]
                      [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the results of the installed Delphix OS versions. Sysadmin type account is needed for this script to run

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

=item B<-report>
Print a verification report of the upgrade check

=item B<-steps>
Print a verification steps of the upgrade check


=item B<-target targetversion>
Limit a verification report output to a target version specified by targetversion


=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLE

Display a list of known Delphix versions

 dx_get_osversions -d Delphix35

 engine name                         name            status                         install date
 ----------------------------------- --------------- ------------------------------ ------------------------------
 53sys                               5.3.0.0         PREVIOUS                       2018-09-28 13:07:51 IST
 53sys                               5.3.2.0         PREVIOUS                       2019-02-15 14:57:16 GMT
 53sys                               5.3.3.0         CURRENTLY_RUNNING              2019-04-12 12:05:47 IST


 dx_get_osversions -d myenginesys -steps -target 6.0.10.1

 engine name                         from            to              step                           status          Start test date
 ----------------------------------- --------------- --------------- ------------------------------ --------------- ------------------------------
 myenginesys                         6.0.4.2         6.0.10.1
                                                                     InitializationRunReport        SUCCESS         2022-02-02 12:33:36 GMT
                                                                     FileSystemCheckRunReport       SUCCESS         2022-02-02 12:46:01 GMT
                                                                     Domain0ThresholdCheck          SUCCESS         2022-02-02 12:46:12 GMT
                                                                     MigrationRunReport             SUCCESS         2022-02-02 12:46:12 GMT
                                                                     ReplicationTargetCheck         SUCCESS         2022-02-02 12:46:21 GMT
                                                                     DeprecatedRulesetEditOptionsCh SKIPPED         2022-02-02 12:46:21 GMT
                                                                     PrimaryDbFileAvailabilityCheck SKIPPED         2022-02-02 12:46:21 GMT
                                                                     EnclosureEscapeCharacterCheck  SKIPPED         2022-02-02 12:46:21 GMT
                                                                     ReplicationSourceCheck         SUCCESS         2022-02-02 12:46:21 GMT
                                                                     HotfixCheck                    SUCCESS         2022-02-02 12:46:21 GMT
                                                                     SnapdirsVisibilityCheck        SUCCESS         2022-02-02 12:46:21 GMT


 dx_get_osversions -d mask -report -target 6.0.15.0

 engine name                         to              title                          status          severity
 ----------------------------------- --------------- ------------------------------ --------------- ---------------
 mask                                6.0.15.0
                                                     Legacy algorithms are being re ACTIVE          WARNING
                                                     Legacy algorithms are being re ACTIVE          WARNING

=cut
