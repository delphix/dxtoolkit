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
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);

if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();

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



for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work

  my $status = "UP";
  if ($engine_obj->dlpx_connect($engine)) {
    $status = "DOWN";
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
    next;
  };

  # load objects for current engine
  my $system = new System_obj( $engine_obj, $debug);
  
  print $system;
  
  exit;

  my $databases = new Databases( $engine_obj, $debug);

  my @vdb = $databases->getDBByType('VDB'); 
  my @dsource = $databases->getDBByType('dSource'); 



  my $storageinfo = $system->getStorage();
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

Toolkit_helpers::print_output($output, $format, $nohead);



__DATA__

=head1 SYNOPSIS

dx_get_appliance.pl [ -d <delphix identifier> | -all ] [ -format csv|json ] [ -nohead ] [ -help|? ]

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head1 OPTIONS

=over 4

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



=cut



