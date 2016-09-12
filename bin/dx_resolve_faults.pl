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
# Program Name : dx_resolve_faults.pl
# Description  : Resolve Delphix Engine faults
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
# Modified     : 20 Jul 2015 (v2.0.0) Marcin Przepiorowski
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
use Faults_obj;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'severity=s' => \(my $severity),
  'target=s' => \(my $target),
  'status=s' => \(my $status), 
  'fault=s' => \(my $fault),
  'ignore' => \(my $ignore),  
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug),
  'format=s' => \(my $format)
) or pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);

pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj= new Engine ($dever, $debug);

my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}


if (defined($status) && ( ! ( (uc $status eq 'ACTIVE') || (uc $status eq 'RESOLVED') ) ) ) {
  print "Option status can have only ACTIVE and RESOLVED value\n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}

if (defined($severity) && ( ! ( (uc $severity eq 'WARNING') || (uc $severity eq 'CRITICAL') ) ) ) {
  print "Option severity can have only WARNING and CRITICAL value\n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}

if ((!defined($severity)) && (!defined($fault)) && (!defined($status))) {
  print "Please define a filter for faults to resolve\n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1); 
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


$output->addHeader(
    {'Appliance',  20},
    {'Fault ref',  20},
    {'Resolved ',  10}
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  if (defined($ignore)) {
  # this is for 4.2 >
    if ($engine_obj->getApi() lt '1.5') {
      print "Option ignore is allowed for Delphix Engine version 4.2 or higher\n";
      pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
      exit (1); 
    }

  }


  if (! defined($st)) {
      # take engine time minus 5 days
    $st = $engine_obj->getTime(24*60*7);
  } else {
    # changing to DE timezone
    $st = Toolkit_helpers::timestamp_to_timestamp_with_de_timezone($st, $engine_obj);
  }
  
  my $st_timestamp;

  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj))) {
    print "Wrong start time (st) format \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);  
  }

  my $et_timestamp;

  if (defined($et)) {
    $et = Toolkit_helpers::timestamp_to_timestamp_with_de_timezone($et, $engine_obj);
    if (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj))) {
      print "Wrong end time (et) format \n";
      pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
      exit (1);  
    } 
  }

  my $faults = new Faults_obj($engine_obj, $st_timestamp, $et_timestamp,  uc $status, uc $severity , $debug);

  if (defined($fault) && (lc $fault eq 'all')) {

    for my $faultitem ( @{ $faults->getFaultsList('asc') } ) {

      my $faultTarget = $faults->getTarget($faultitem);
      
      if (defined($target)) {

        # if like is defined we are going to resolve only ones maching like
        if ( ! ($faultTarget =~ m/\Q$target/)  ) {
          next;
        } 

      }

      if ($faults->resolveFault($faultitem, $ignore)) {
        $output->addLine(
          $engine,
          $faultitem,
          "ERROR"
        );
      } else {
        $output->addLine(
          $engine,
          $faultitem,
          $faults->getStatus($faultitem)
        );      
      }

    }
  } else {
    if ($faults->resolveFault($fault, $ignore)) {
      $output->addLine(
        $engine,
        $fault,
        "ERROR"
      );
    } else {
      $output->addLine(
        $engine,
        $fault,
        $faults->getStatus($fault)
      );      
    }
  }
}

Toolkit_helpers::print_output($output, $format, $nohead);



__DATA__

=head1 SYNOPSIS

 dx_resolve_faults.pl [ -engine|d <delphix identifier> | -all ] [-fault FAULTREF | all] [-st timestamp] [-et timestamp] [-severity severity] [-status status] [-target target]
                  [ -format csv|json ]  [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Resolve faults of Delphix Engine defined by filters

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Resolve faults on all Delphix appliance


=back

=head2 Filters

=over 4

=item B<-fault faultref | all>
Resolve fault defined by fault ref or all faults limited by filter


=back

Filter faults using one of the following filters if fault parameter is set to all

=over 4

=item B<-severity>
Fault severity - WARNING / CRITICAL

=item B<-status>
Fault status - ACTIVE / RESOLVED

=item B<-target>
Fault target ( VDB name, target host name)

=back

=head1 OPTIONS

=over 3


=item B<-st timestamp>
Start time for faults list - default value is 7 days

=item B<-et timestamp>
End time for faults list 

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



