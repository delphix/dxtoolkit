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
#
# Program Name : dx_get_event.pl
# Description  : Get Delphix Engine audit
# Author       : Marcin Przepiorowski
# Created      : 22 Sep 2016 (v2.0.0)
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
use Alert_obj;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'format=s' => \(my $format), 
  'outdir=s' => \(my $outdir),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug)
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


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


$output->addHeader(
    {'Appliance',             20},
    {'Alert',                 20},
    {'Action',                20},
    {'Response',              20},
    {'Target name',           25}, 
    {'Timestamp',             35},
    {'Serverity',             15},
    {'Title',                 20},
    {'Description',           50}
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  if (! defined($st)) {
      # take engine time minus 1 day
    $st = $engine_obj->getTime(24*60);
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

  my $alerts = new Alert_obj($engine_obj, $st_timestamp, $et_timestamp, $debug);

  for my $alertitem ( @{$alerts->getAlertList('asc')} ) {



    $output->addLine(
      $engine,
      $alertitem,
      $alerts->getEventAction($alertitem),
      $alerts->getEventResponse($alertitem),
      $alerts->getTargetName($alertitem),
      $alerts->getTimeStampWithTZ($alertitem),
      $alerts->getEventSeverity($alertitem),
      $alerts->getEventTitle($alertitem),
      $alerts->getEventDesc($alertitem)
    )

  }
}

if (defined($outdir)) {
  Toolkit_helpers::write_to_dir($output, $format, $nohead,'events',$outdir,1);
} else {
  Toolkit_helpers::print_output($output, $format, $nohead);
}



__DATA__

=head1 SYNOPSIS

 dx_get_event.pl [ -engine|d <delphix identifier> | -all ] 
                 [-st timestamp] 
                 [-et timestamp] 
                 [-format csv|json ]  
                 [-outdir path]
                 [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of events from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head1 OPTIONS

=over 3


=item B<-st timestamp>
Start time for event list - default value is 7 days
Timestampt format is "YYYY-MM-DD [HH24:MI:[SS]]"

=item B<-et timestamp>
End time for event list 
Timestampt format is "YYYY-MM-DD [HH24:MI:[SS]]"

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




=cut



