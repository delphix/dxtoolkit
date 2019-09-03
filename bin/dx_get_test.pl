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
# Program Name : dx_get_appliance.pl
# Description  : Get appliance information
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
# Modified     : 08 Jun 2015 (v2.0.0) Marcin Przepiorowski
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
use System_obj;
use Databases;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
  'details' => \(my $details),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
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

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

if (defined($details)) {
  $output->addHeader(
    {'Appliance', 20},
    {'Status',  8},
    {'Version', 8},
    {'Total (GB)', 10},
    {'Used (GB)',  10},
    {'Free (GB)',  10},
    {'PctUsed(%)', 10},
    {'dSource#',   8},
    {'VDBs#',      8},
    {'Total Objects', 8},
    {'vCpu',  8},
    {'vMem [GB]',  9}
  );
} else {
  $output->addHeader(
    {'Appliance', 20},
    {'Status',  8},
    {'Version', 8},
    {'Total (GB)', 10},
    {'Used (GB)',  10},
    {'Free (GB)',  10},
    {'PctUsed(%)', 10},
    {'dSource#',   8},
    {'VDBs#',      8},
    {'Total Objects', 8}
  );
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work

#   client_id="0oa4qivm0j5dwndsk2p7"
# client_secret="Krl-UymYE6OP-z_bhT5sAI0kSCPaz0u9VKtLhRTn"

  my $token = $engine_obj->getSSOToken("0oa4qivm0j5dwndsk2p7", "Krl-UymYE6OP-z_bhT5sAI0kSCPaz0u9VKtLhRTn");
  if (defined($token)) {
    print "I have a token";
    $token = $engine_obj->dlpx_connect($engine, $token);
  } else {
    print "Can't get token from token provider\n";
    exit(-1);
  }

  my $status = "UP";
  if ($engine_obj->dlpx_connect($engine)) {
    $status = "DOWN";
    if (defined($details)) {
      $output->addLine(
        $engine,
        $status,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        ""
      );
    } else {
      $output->addLine(
        $engine,
        $status,
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        ""
      );
    }
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine
  my $system = new System_obj( $engine_obj, $debug);
  my $databases = new Databases( $engine_obj, $debug);

  my @vdb = $databases->getDBByType('VDB');
  my @dsource = $databases->getDBByType('dSource');



  my $storageinfo = $system->getStorage();

  if (defined($details)) {
    $output->addLine(
      $engine,
      $status,
      $system->getVersion(),
      $storageinfo->{Total},
      $storageinfo->{Used},
      $storageinfo->{Free},
      $storageinfo->{pctused},
      scalar(@dsource),
      scalar(@vdb),
      scalar(@dsource) +  scalar(@vdb),
      $system->getvCPU(),
      sprintf("%8.2f",$system->getvMem())
    );
  } else {
    $output->addLine(
      $engine,
      $status,
      $system->getVersion(),
      $storageinfo->{Total},
      $storageinfo->{Used},
      $storageinfo->{Free},
      $storageinfo->{pctused},
      scalar(@dsource),
      scalar(@vdb),
      scalar(@dsource) +  scalar(@vdb)
    );
  }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

dx_get_appliance [-d <delphix identifier> | -all ]
                 [-format csv|json ]
                 [-nohead ]
                 [-details ]
                 [-help|? ]

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

=over 4

=item B<-details>
Display vCPU and vMemory

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-nohead>
Turn off header

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display a Delphix Engine summary

 dx_get_appliance -d Landshark5

 Appliance            Status   Version  Total (GB) Used (GB)  Free (GB)  PctUsed(%) dSource# VDBs#    Total Ob
 -------------------- -------- -------- ---------- ---------- ---------- ---------- -------- -------- --------
 Landshark5           UP       5.0.5.1  28.82      5.20       23.62      0.18       8        4        12


=cut
