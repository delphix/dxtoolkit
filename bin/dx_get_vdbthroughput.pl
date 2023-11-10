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
# Copyright (c) 2018 by Delphix. All rights reserved.
#
# Program Name : dx_get_vdbthroughput.pl
# Description  : Get Delphix Engine database performance
# Author       : Marcin Przepiorowski
# Created      : 27 Aug 2015 (v2.3.7) Marcin Przepiorowski


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
use PerfHistory_obj;
use Databases;

my $version = $Toolkit_helpers::version;
my $interval = 60;
my $format = "csv";

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'st=s' => \(my $st),
  'et=s' => \(my $et),
  'i=s' => \($interval),
  'outdir=s' => \(my $outdir),
  'format=s' => \($format),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug),
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

if (($interval != 60) && ($interval != 3600) && ($interval != 1)) {
  print "Interval can be only 60 or 3600 seconds\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ((lc $format ne "pretty") && (!defined($outdir))) {
  print "Outdir is required for JSON and CSV output\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
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

  my $output = new Formater();

  my $st_timestamp;

  print Dumper "ROBIE ST";

  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj, 1))) {
    print "Wrong start time (st) format \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }


  print Dumper "END ST";

  my $et_timestamp;

  if (defined($et)) {
    if (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj, 1))) {
      print "Wrong end time (et) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);
    }
  } else {
    $et = "-0min";
    if (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj, 1))) {
      print "Wrong start time (st) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);
    }
  }

  print Dumper $st_timestamp;
  print Dumper $et_timestamp;


  my $db = new Databases ( $engine_obj, $debug );

  my $noofdbs = scalar(keys(%{$db->{_dbs}}));

  my $dbobj;

  my $perfhist = new PerfHistory_obj($engine_obj, $st_timestamp, $et_timestamp, $interval, $noofdbs, $debug);

  my $perfdata = $perfhist->returndata();


  # check if no data returned
  my $firstts = (keys(%{$perfdata}))[0];

  if (defined($firstts)) {

    my @dbnamelist;
    push(@dbnamelist, {'timestamp', '30'});

    for my $dbref (sort(keys(%{$perfdata->{$firstts}}))) {
      $dbobj = $db->getDB($dbref);
      push(@dbnamelist, {$dbobj->getName(), '30'});
    }

    $output->addHeader(
      @dbnamelist
    );


    for my $ts (sort (keys(%{$perfdata}))) {
      my @tarray = map { $perfdata->{$ts}->{$_} } sort(keys(%{$perfdata->{$ts}}));
      my @fullarray = ($ts, @tarray);
      $output->addLine(@fullarray);
    }

    if (defined($outdir)) {
      Toolkit_helpers::write_to_dir($output, $format, $nohead,$engine . '-vdbthroughput',$outdir,1);
    } else {
      Toolkit_helpers::print_output($output, $format, $nohead);
    }
  } else {
    print "No data found. Please check start/end date and interval\n";
    $ret = 1;
  }

}



exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_vdbthroughput    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                         [-st timestamp]
                         [-et timestamp]
                         [-i 1|60|3600]
                         [-outdir path]
                         [ --help|? ]
                         [ -debug ]

=head1 DESCRIPTION

Get the VDB throughput split based on last day data

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script for all Delphix Engines from config file

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back


=head1 OPTIONS

=over 3


=item B<-st timestamp>
Start time for performance data - default value is -1 day

=item B<-et timestamp>
End time for peformance data - default is now

=item B<-outdir path>
Write output into a directory specified by path.

=item B<-i interval>
Use the specified interval for export. Allowed values are:
1, 60, 3600

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

Export last 60 min of data using 60 sec interval

 dx_get_vdbthroughput -d 53 -outdir /tmp/ -st -60mins
 Data exported into /tmp/53-vdbthroughput-20190320-12-48-25.csv

Export all possible data using a 60 sec interval

  dx_get_vdbthroughput -d 53 -outdir /tmp/
  Data exported into /tmp/53-vdbthroughput-20190320-12-48-33.csv

Export last 60 min of data using 1 sec interval

 dx_get_vdbthroughput -d 53 -outdir /tmp/ -st -60mins -i 1
 Data exported into /tmp/53-vdbthroughput-20190320-12-52-05.csv

=cut
