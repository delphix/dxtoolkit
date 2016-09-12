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

my ($sec,$min,$hour,$mday,$mon,$year) = localtime(time()-604800);
$mon = $mon + 1;
# default start time is -6 min ( 5 min resolution for 60 sec interval)
my $st = $year+1900 . "-" . sprintf("%02d",$mon) . "-" . sprintf("%02d",$mday) . " " . sprintf("%02d",$hour) . ":" . sprintf("%02d",$min) . ":" . sprintf("%02d",$sec);

# default resolution is 1 h
my $resolution = '3600';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'format=s' => \(my $format), 
  'debug:i' => \(my $debug), 
  'all' => (\my $all),
  'type|t=s' => (\my $type),
  'outdir=s' => \(my $outdir), 
  'st=s' => \($st), 
  'et=s' => \(my $et), 
  'dever=s' => \(my $dever),
  'interval|i=s' => \($resolution), 
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


my %allowedres = (
        '1' => 'S',
        '60' => 'M',
        '3600' => 'H',
        'H' => 'H',
        'S' => 'S',
        'M' => 'M'
        );

my %convertres = (
        'S' => '1',
        'M' => '60',
        'H' => '3600',
        );

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

if (! defined($type) ) {
  print "Parameter type is required \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);  
}

if (! defined($outdir) ) {
  print "Parameter outdir is required \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);  
}

if (! defined( $allowedres{$resolution} ) ) {
  print "Wrong -i parameter \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit(1);
}

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    next;
  } else {
    print "Connected to Delphix Engine $engine (IP " . $engine_obj->getIP() .")\n\n";
  }

  my $st_timestamp;

  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj))) {
    print "Wrong start time (st) format \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);  
  }

  my $et_timestamp;

  if (defined($et) && (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj)))) {
    print "Wrong end time (et) format \n";
    pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
    exit (1);  
  }

  my $analytic_list = new Analytics($engine_obj, $debug);





  if ( defined( $convertres{$resolution} ) ) {
    $resolution = $convertres{$resolution};
  }




  my $arguments = "&resolution=$resolution&numberofDatapoints=10000&startTime=$st_timestamp";
  my $endTime = $et_timestamp ? "&endTime=$et_timestamp" : "";
  $arguments = $arguments . $endTime;
  $analytic_list->get_perf($type, $outdir, $arguments, $allowedres{$resolution}, $format );

}










__DATA__

=head1 SYNOPSIS

  dx_get_analytics.pl ( -d <delphix identifier> | -all )
                        -type <cpu|disk|nfs|iscsi|network|nfs-by-client|nfs-all|all|standard|comma separated names>
                        -outdir <output dir>
                        [-i interval ] 
                        [-st <start_time> ]  
                        [-et <end_time> ]
                        -format csv|json
                        -debug


=head1 DESCRIPTION

Get analytics collector inside Delphix Engine

=head1 ARGUMENTS

=over 4

=item B<-d>
Delphix Identifier (hostname defined in dxtools.conf) 

=item B<-st>
Start time (format: "DD-MON-YYYY [HH24:MI:SS] or "YYYY-MM-DD [HH24:MI:SS]" ). Default start time is today minus 7 days

=item B<-et>
End time (format: "DD-MON-YYYY [HH24:MI:SS]" or "YYYY-MM-DD [HH24:MI:SS]" )

=item B<-t>
Type: cpu|disk|nfs|iscsi|network|nfs-by-client|nfs-all|all|standard|comma separated names

ex.
-t all - for all analytics

-t standard - for cpu,disk,network and nfs analytics

-t cpu,disk - for cpu and disk

=item B<-i>
Time Inteval, allowed values are 1 or S for 1 sec, 60 or M for 1 min , 3600 or H for 1 hour

=item B<-outdir>
Output directory

=back

=head1 OPTIONS

=over 4

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified csv formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>          
Turn on debugging

=back