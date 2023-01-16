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

my $output_unit = 'G';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
  'details' => \(my $details),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'output_unit:s' => \($output_unit),
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
    {Toolkit_helpers::get_unit('Total',$output_unit), 10},
    {Toolkit_helpers::get_unit('Used',$output_unit),  10},
    {Toolkit_helpers::get_unit('Free',$output_unit),  10},
    {'PctUsed(%)', 10},
    {'dSource#',   8},
    {'VDBs#',      8},
    {'Total Objects', 8},
    {'vCpu',  8},
    {'vMem [GB]',  9},
    {'UUID',40},
    {'Type',20}
  );
} else {
  $output->addHeader(
    {'Appliance', 20},
    {'Status',  8},
    {'Version', 8},
    {Toolkit_helpers::get_unit('Total',$output_unit), 10},
    {Toolkit_helpers::get_unit('Used',$output_unit),  10},
    {Toolkit_helpers::get_unit('Free',$output_unit),  10},
    {'PctUsed(%)', 10},
    {'dSource#',   8},
    {'VDBs#',      8},
    {'Total Objects', 8}
  );
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work

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
      Toolkit_helpers::print_size($storageinfo->{Total}, 'G', $output_unit), 
      Toolkit_helpers::print_size($storageinfo->{Used}, 'G', $output_unit), 
      Toolkit_helpers::print_size($storageinfo->{Free}, 'G', $output_unit), 
      $storageinfo->{pctused},
      scalar(@dsource),
      scalar(@vdb),
      scalar(@dsource) +  scalar(@vdb),
      $system->getvCPU(),
      sprintf("%8.2f",$system->getvMem()),
      $system->getUUID(),
      $system->getEngineType()
    );
  } else {
    $output->addLine(
      $engine,
      $status,
      $system->getVersion(),
      Toolkit_helpers::print_size($storageinfo->{Total}, 'G', $output_unit), 
      Toolkit_helpers::print_size($storageinfo->{Used}, 'G', $output_unit), 
      Toolkit_helpers::print_size($storageinfo->{Free}, 'G', $output_unit), 
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
                  [-output_unit K|M|G|T]
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

=item B<-output_unit K|M|G|T>
Display usage using different unit. By default GB are used
Use K for KiloBytes, G for GigaBytes and M for MegaBytes, T for TeraBytes

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

=head1 COLUMNS

Columns description

=over 1

=item B<Appliance> - Delphix Engine name from dxtools.conf file

=item B<Status> - Status of Delphix Engine

=item B<Version> - Version of Delphix Engine

=item B<Total> - Total storage allocated to Delphix Engine

=item B<Used> - Used space. Up to version 6.0.7 this is space used by data objects. Since 6.0.7 this include a reserved space as well.

=item B<Free> - Free space on engine

=item B<PctUsed> - Percent of used space

=item B<dSource#> - Number of dSources on the engine

=item B<VDBs#> - Number of VDBs on the engine

=item B<Total Objects> - Total number of objects on the engine

=item B<vCpu> - No of vCPU allocated to the engine

=item B<vMem> - Size of memory allocated to the engine

=back


=head1 EXAMPLES

Display a Delphix Engine summary

 dx_get_appliance -d Landshark5

 Appliance            Status   Version  Total (GB) Used (GB)  Free (GB)  PctUsed(%) dSource# VDBs#    Total Ob
 -------------------- -------- -------- ---------- ---------- ---------- ---------- -------- -------- --------
 Landshark5           UP       5.0.5.1  28.82      5.20       23.62      0.18       8        4        12

Display a Delphix Engine details 

 dx_get_appliance.pl -d dxt1 -details

 Appliance            Status   Version  Total (GB) Used (GB)  Free (GB)  PctUsed(%) dSource# VDBs#    Total Ob vCpu     vMem [GB] UUID
 -------------------- -------- -------- ---------- ---------- ---------- ---------- -------- -------- -------- -------- --------- ----------------------------------------
 dxt1                 UP       6.0.12.1 21.80      7.62       14.18      34.96      1        0        1        2            8.00  564d754d-eb0f-bb3e-15a2-f45c08d0ae24

=cut
