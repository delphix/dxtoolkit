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
# Program Name : dx_get_disk_throughput.pl
# Description  : Get disk throughput
#
# Modified     : 04 Jun 2015 (v2.0.0) Marcin Przepiorowski
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
use Analytics;
use Formater;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

# default resolution is 60 sec
my $resolution = '1';

my $warn = 100;
my $crit = 300;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host),
  'all' => \(my $all), 
  'debug:i' => \(my $debug), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'w=i' => \($warn),
  'c=i' => \($crit),
  'read' => \(my $read),
  'write' => \(my $write),
  'opname=s' => \(my $opname),
  'raw' => \(my $raw),
  'interval|i=s' => \($resolution), 
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);

$opname = Toolkit_helpers::opname_options($opname, $read, $write);

my %allowedres = (
        '1' => 'S',
        '60' => 'M',
        '3600' => 'H',
        'H' => 'H',
        'M' => 'M',
        'S' => 'S'
        );

if (!defined( $allowedres{$resolution} )) {
  print "Wrong interval \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (3);    
}



# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

if (scalar(@{$engine_list}) > 1) {
  print "More than one engine is default. Use -d parameter\n";
  exit(3);
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
    $st = "-5min";
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
  my $name = "disk";
  my $metric;

  if (lc $opname eq 'w') {
    $metric = "throughput_w";
  } 
  elsif (lc $opname eq 'r') {
    $metric = "throughput_r";
  } else {
    $metric = "throughput_t";
  }

  my $arguments = "&resolution=$resolution&numberofDatapoints=10000&startTime=$st_timestamp";
  my $endTime = $et_timestamp ? "&endTime=$et_timestamp" : "";
  $arguments = $arguments . $endTime;
  
  Toolkit_helpers::nagios_check($engine, $analytic_list, $name, $metric, $arguments, $allowedres{$resolution}, $raw, $crit, $warn);

}





__DATA__

=head1 SYNOPSIS

 dx_get_disk_throughput -d <delphix identifier> 
             [-w <warning millisec>] 
             [-i time_interval] 
             [-c <critical millisec>]
             [-opname operation] 
             [-read | -write] 
             [-raw ] 
             [-st "YYYY-MM-DD [HH24:MI:SS]" ] 
             [-et "YYYY-MM-DD [HH24:MI:SS]" ] 
             [ -debug ] 
             [ -help|-? ]


=head1 ARGUMENTS

=over 4

=item B<-d>
Delphix Identifier (hostname defined in dxtools.conf) 

=back

=head1 OPTIONS

=over 4

=item B<-st>
StartTime (format: YYYY-MM-DD [HH24:MI:SS]). Default is "now-5 min".

=item B<-et>
EndTime (format: YYYY-MM-DD [HH24:MI:SS]). Default is "now"

=item B<-i>
Time Inteval, allowed values are 1 or S for 1 sec, 60 or M for 1 min , 3600 or H for 1 hour

=item B<-opname r|w|b >
Operation name r for read, w for write, b for both (default value)

=item B<-write >
Old syntax. Similar to -opname w

=item B<-read >
Old syntax. Similar to -opname r

=item B<-w>
Warning level in MB/s (Integer, Default 100)

=item B<-c>
Critical level in MB/s (Integer, Default 300)

=item B<-raw>
Show Raw Data, instead of average

=item B<-help>          
Print this screen

=item B<-debug>          
Turn on debugging

=back

=head1 EXAMPLES

Average disk throughput for a last 5 minutes using 1-second sample

 dx_get_disk_throughput -d DE1
 WARNING:DE1 disk throughput MB/s 107.88

Average disk throughput for a last 5 minutes using 1-second sample with warning set to 200 MB/s

 dx_get_disk_throughput -d DE1 -w 200
 OK:DE1 disk throughput MB/s 105.07


=cut

