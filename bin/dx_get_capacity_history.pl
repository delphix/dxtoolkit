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
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

my $resolution = 'd';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'format=s' => \(my $format), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'debug:i' => \(my $debug), 
  'details' => \(my $details),
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

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();


if (defined($details)) {
  $output->addHeader(
    {'Engine',         30},
    {'Timestamp',      30},
    {'dS total [GB]',  15},
    {'dS current [GB]',15},
    {'dS log [GB]'    ,15},
    {'dS snaps [GB]'  ,15},
    {'VDB total [GB]' ,15},
    {'VDB current [GB]',15},
    {'VDB log [GB]'   ,15},
    {'VDB snaps [GB]' ,15},
    {'Total [GB]',     15},
    {'Usage [%]',      12}
  );
} else {
  $output->addHeader(
    {'Engine',         30},
    {'Timestamp',      30},
    {'dSource [GB]',   12},
    {'Virtual [GB]',   12},
    {'Total [GB]',     12},
    {'Usage [%]'     , 12}
  );
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
  my $capacity = new Capacity_obj($engine_obj, $debug);
  #$capacity->LoadDatabases();
  $capacity->LoadSystemHistory($st_timestamp, $et_timestamp, $reshash{$resolution});
  $capacity->processSystemHistory($output,$details);


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

=cut



