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
# Program Name : dx_get_replication.pl
# Description  : Get information about replication
# Author       : Marcin Przepiorowski
# Created      : 28 Sept 2015 (v2.2.0)
#
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
use Replication_obj;
use Formater;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host),
  'list' => \(my $list),
  'last' => \(my $last),
  'format=s' => \(my $format), 
  'debug:i' => \(my $debug), 
  'cron'    => \(my $cron),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();

if (defined($last) && (defined($list))) {
  print "Options -last and -list are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($last)) {
  $output->addHeader(
      {'Appliance',          10},
      {'Profile name',       20},
      {'Replication target', 20},
      {'Enable',              9}
  );  
} elsif (defined($list)) {
  $output->addHeader(
      {'Appliance',          10},
      {'Profile name',       20},
      {'Replication target', 20},
      {'Enable',              9}
  );
} else {
  $output->addHeader(
      {'Appliance',          10},
      {'Profile name',       20},
      {'Replication target', 20},
      {'Enable',              9},
      {'Last Run',           20},
      {'Status',             15},
      {'Schedule',           40},
      {'Run Time',           10},
      {'Next run',           20},
      {'Objects',            20}
  );
}

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    #print "Can't connect to Dephix Engine $engine\n\n";
    next;
  };

  my $replication = new Replication_obj( $engine_obj, $debug );


  for my $repitem ( $replication->getReplicationList() ) {

    if (defined($list)) {
      $output->addLine(
        $engine,
        $replication->getName($repitem),
        $replication->getTargetHost($repitem),
        $replication->getEnabled($repitem)
      );
    } else {

      my $schedule = ($replication->getLastJob($repitem))->{'Schedule'};

      $output->addLine(
        $engine,
        $replication->getName($repitem),
        $replication->getTargetHost($repitem),
        $replication->getEnabled($repitem),
        ($replication->getLastJob($repitem))->{'StartTime'},
        ($replication->getLastJob($repitem))->{'State'},
        defined($cron) ? $schedule : Toolkit_helpers::parse_cron($schedule),
        ($replication->getLastJob($repitem))->{'Runtime'},
        ($replication->getLastJob($repitem))->{'NextRun'},
        $replication->getObjectsName($repitem)
      );
    }
  }



}


Toolkit_helpers::print_output($output, $format, $nohead);



__DATA__

=head1 SYNOPSIS

 dx_get_replication.pl [ -engine|d <delphix identifier> | -all ] [-cron] [ -format csv|json ]  [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about engine replication.

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

=item B<-cron>
Display schedule using a cron expression

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-details>
Display more information about database capacity

=item B<-nohead>
Turn off header output

=back




=cut



