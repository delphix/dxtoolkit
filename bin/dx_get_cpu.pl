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
# Program Name : dx_get_analytics.pl
# Description  : Get analytics information from Delphix Engine
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
#
# Modified     : 27 May 2015 (v2.0.0) Marcin Przepiorowski
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
use Analytics;
use Formater;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

# default resolution is 1 sec
my $resolution = '1';

my $warn = 75;
my $crit = 95;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'all' => (\my $all),
  'debug:i' => \(my $debug), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'w=i' => \($warn),
  'c=i' => \($crit),
  'raw' => \(my $raw),
  'dever=s' => \(my $dever),
  'interval|i=s' => \($resolution), 
  'version' => \(my $print_version),
  'nohead' => \(my $nohead)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);



my %allowedres = (
        '1' => 'S',
        '60' => 'M',
        '3600' => 'H',
        'H' => 'H',
        'M' => 'M',
        'S' => 'S'
        );

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

if (scalar(@{$engine_list}) > 1) {
  print "More than one engine is default. Use -d parameter\n";
  exit(3);
}

if (!defined( $allowedres{$resolution} )) {
  print "Wrong interval \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (3);    
}

# End of script parametes checks

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    exit(3);
  } 

  my $st_timestamp;

  if (! defined($st)) {
    # take engine time minus 5 min
    $st = $engine_obj->getTime('5');
  }

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


  my $analytic_list = new Analytics($engine_obj, $debug);
  my $name = "cpu";
  my $metric = "utilization";

  my $arguments = "&resolution=$resolution&numberofDatapoints=10000&startTime=$st_timestamp";
  my $endTime = $et_timestamp ? "&endTime=$et_timestamp" : "";
  $arguments = $arguments . $endTime;
  
  Toolkit_helpers::nagios_check($engine, $analytic_list, $name, $metric, $arguments, $allowedres{$resolution}, $raw, $crit, $warn);

}







__DATA__

=head1 SYNOPSIS

dx_get_cpu -d <delphix identifier> 
           [ -w <warning % used>  ] 
           [ -c <critical % used> ]
           [ -raw ] 
           [ -st "DD-MON-YYYY [HH24:MI:SS]" ] 
           [ -et "DD-MON-YYYY [HH24:MI:SS]" ] 
           [ -debug ] 
           [ -help|-? ]


=head1 ARGUMENTS

 -help,-?          Print this screen
 -d                 Delphix Server (from dxtools.conf)

=head1 OPTIONS

 -w                 Warning level % Used (Integer, Default 75)
 -c                 Critical level % Used (Integer, Default 95)
 -st                Start Time in format "DD-MON-YYYY [HH24:MI:SS]" (Optional)
 -et                End Time in format "DD-MON-YYYY [HH24:MI:SS]" (Optional, default "now")
 -raw               Show Raw Data, instead of average
 -debug             Show Debug info


=cut
