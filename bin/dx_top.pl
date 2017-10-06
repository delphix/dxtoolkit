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
# Program Name : dx_top.pl
# Description  : Get analytics information from Delphix Engine
# Author       : Marcin Przepiorowski
# Created      : 20 Apr 2016 (v2.2.0)
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
use Date::Manip;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Analytics;
use Formater;
use Toolkit_helpers;
use URI::Escape;

my $version = $Toolkit_helpers::version;

# default resolution is 1 sec
my $resolution = '3600';
my $stat = 't';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'debug:i' => \(my $debug), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'loop=i' => \(my $loop),
  'stat=s' => \($stat),
  'dever=s' => \(my $dever),
  'interval|i=s' => \($resolution), 
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
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
my $engine_list = Toolkit_helpers::get_engine_list(undef, $dx_host, $engine_obj); 

if (scalar(@{$engine_list}) > 1) {
  print "More than one engine is default. Use -d parameter\n";
  exit(3);
}

if (!defined( $allowedres{$resolution} )) {
  print "Wrong interval \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (3);    
}


if ( ! ( (defined($et) && defined($st) || defined($loop) ) ) ) {
  print "Paramerers st and et or loop are required \n";
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
    $st = "-5min";
  }

  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st,$engine_obj))) {
    print "Wrong start time (st) format $st\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (3);  
  }

  my $et_timestamp;
  my $endtime;

  if (defined($et) && (! defined($et_timestamp = Toolkit_helpers::timestamp($et,$engine_obj)))) {
    print "Wrong end time (et) format $et\n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (3);  
  }


  # print Dumper $et;

  my $detz = $engine_obj->getTimezone();
  my $tz = new Date::Manip::TZ;
# auto

  # my $dt = new Date::Manip::Date;
  # $dt->config("setdate","zone," . $detz);
  # my $err = $dt->parse($et);
  # $endtime = $dt->value();

  # print Dumper $endtime;

  my $count = 99;
  my $max = 100;





  if (defined($loop)) {

    my $time;
    my $operation = "resources/json/service/configure/currentSystemTime";
    my ($result,$result_fmt, $retcode) = $engine_obj->getJSONResult($operation);
    my $starttime;
    if ($result->{result} eq "ok") {
      $time = $result->{systemTime}->{localTime};
      $time =~ s/\s[A-Z]{1,3}$//;
      $endtime = ParseDate($time);
      $starttime = DateCalc(ParseDate($time), ParseDateDelta('- 5 second'));

    } else {
      $time = 'N/A';
    } 


    $count = 0;
    $max = $loop;
    $resolution = 1;



    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($starttime, $detz);
    my $tstz = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
    $st_timestamp = uri_escape($tstz);

    ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($endtime, $detz);
    $tstz = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);

    my $dt = new Date::Manip::Date;
    $dt->config("setdate","zone,GMT");
    $err = $dt->parse($tstz);
    $endtime = $dt->value();
    $et_timestamp = uri_escape($tstz);

  }

#auto

  my $analytic_list = new Analytics($engine_obj, $debug);

  my $cpu = $analytic_list->getAnalyticByName('cpu');
  my $disk = $analytic_list->getAnalyticByName('disk');
  my $nfs = $analytic_list->getAnalyticByName('nfs');

  if ($stat eq 't') {
    printf("%20s  %12s %15s %20s %12s %12s  \n", "", "CPU", "", "Disk throughput", "", "NFS throughput");
  } else {
    printf("%20s  %12s %15s %20s %12s %12s  \n", "", "CPU", "", "Disk latency", "", "NFS latency");
  }
  printf("%20s : %5s %5s %5s %5s : %6s %6s %6s %6s : %6s %6s %6s %6s\n", "Timestamp", "avg", "min", "max", "85pct", 
    "avg", "min", "max", "85pct","avg", "min", "max", "85pct");


  while ($max > $count) {
    $count++;

    my $arguments = "&resolution=$resolution&numberofDatapoints=1000&startTime=$st_timestamp&endTime=$et_timestamp";


    if (defined($loop)) {
      my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($endtime, $detz);
      my $tstz = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
      printData($arguments, $tstz, $cpu, $disk, $nfs, $stat);
      sleep 5;
    } else {
      printData($arguments, $et, $cpu, $disk, $nfs, $stat);
    }

    $endtime = DateCalc($endtime, ParseDateDelta('+ 5 second'));
    $st_timestamp = $et_timestamp;
    $et_timestamp = uri_escape(UnixDate($endtime , "%Y-%m-%dT%H:%M:%S.000Z" ));

  }

}


sub printData {
  my $arguments = shift;
  my $st = shift;
  my $cpu = shift;
  my $disk = shift;
  my $nfs = shift;
  my $stat = shift;


  $cpu->getData($arguments, $resolution);
  $cpu->processData(2);
  #$cpu->doAggregation();

  $disk->getData($arguments, $resolution);
  $disk->processData(2);
  #$disk_throughput->doAggregation();

  $nfs->getData($arguments, $resolution);
  $nfs->processData(2);
  #$nfs_throughput->doAggregation();

  my ($avgcpu, $mincpu, $maxcpu, $per85cpu) = ("","","");
  my ($avgdisk, $mindisk, $maxdisk, $per85disk) = ("","","");
  my ($avgnfs, $minnfs, $maxnfs, $per85nfs) = ("","","");

  if ($stat eq 't') {
    ($avgcpu, $mincpu, $maxcpu, $per85cpu) = $cpu->get_stats('utilization');
    ($avgdisk, $mindisk, $maxdisk, $per85disk) = $disk->get_stats('throughput_t');
    ($avgnfs, $minnfs, $maxnfs, $per85nfs) = $nfs->get_stats('throughput_t');
  } else {
    ($avgcpu, $mincpu, $maxcpu, $per85cpu) = $cpu->get_stats('utilization');
    ($avgdisk, $mindisk, $maxdisk, $per85disk) = $disk->get_stats('latency_t');
    ($avgnfs, $minnfs, $maxnfs, $per85nfs) = $nfs->get_stats('latency_t');   
  }
    
  printf("%20s : %5.2f %5.2f %5.2f %5.2f : %6.2f %6.2f %6.2f %6.2f : %6.2f %6.2f %6.2f %6.2f\n", 
    $st, $avgcpu, $mincpu, $maxcpu, $per85cpu, 
    $avgdisk, $mindisk, $maxdisk, $per85disk, $avgnfs, $minnfs, $maxnfs, $per85nfs);


}




__DATA__

=head1 SYNOPSIS

dx_top [ -engine|d <delphix identifier> ] [ -configfile file ] 
       [ -st "YYYY-MM-DD [HH24:MI:SS]" -et "YYYY-MM-DD [HH24:MI:SS]" ] 
       [-loop no] 
       [-i 1,60,3600]

=head1 DESCRIPTION

Get the information about engine in line format.

=head1 ARGUMENTS

=over 4

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file


=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location


=item B<-stat t|l>
Statictics t - throughput (default), l - latency

=item B<-st>
Start time 

=item B<-et>
End time 

=item B<-loop no>
Number of loops for real time monitoring. There is a 5 seconds delay between checks

=item B<-i 1,60,3600>
Sampling resolution in seconds

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back


=cut
