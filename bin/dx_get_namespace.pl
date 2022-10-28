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
# Program Name : dx_get_namespace.pl
# Description  : Get information about namespaces
# Author       : Marcin Przepiorowski
# Created      : Oct 2022 (v2.4.17)
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
use Namespace_obj;
use Databases;
use Group_obj;
use Formater;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host),
  'backup=s' => \(my $backup),
  'namespacename=s' => \(my $namespacename),
  'format=s' => \(my $format), 
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();

$output->addHeader(
    {'Appliance',          10},
    {'Namespace',          20},
    {'Last complited run',           20},
    {'Objects',            200}
);


my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
  };

  my $namespaces = new Namespace_obj( $engine_obj, $debug );
  my $databases = new Databases( $engine_obj, $debug );
  my $groups = new Group_obj($engine_obj, $debug);
  my @db_list = sort { Toolkit_helpers::sort_by_dbname($a,$b,$databases,$groups, $debug) } $databases->getDBList();


  my %dbmap;
  my $dbns;
  my $dbname;
  my $groupname;
  for my $dbref (@db_list) {
    $dbns = $databases->getDB($dbref)->getNamespace();
    ($dbname) = ($databases->getDB($dbref)->getName()) =~ /(.*)@.*/;
    ($groupname) = ($groups->getName($databases->getDB($dbref)->getGroup())) =~ /(.*)@.*/;
    $dbname = $groupname . "/" . $dbname;
    if (defined($dbmap{$dbns})) {
      push(@{$dbmap{$dbns}}, $dbname);
    } else {
      my @dbarr = ($dbname);
      $dbmap{$dbns} = \@dbarr;
    }
  }

  my $times = $namespaces->findlastreplica();

  for my $spaceitem ( $namespaces->getNamespaceList() ) {

    if (defined($namespacename) && (uc $namespaces->getName($spaceitem) ne uc $namespacename)) {
      $ret = $ret + 1;
      next;
    }

    my $obj;

    if (defined($dbmap{$spaceitem})) {
      $obj = join(',',@{$dbmap{$spaceitem}});
    } else {
      $obj = 'N/A';
    }

    $output->addLine(
      $engine,
      $namespaces->getName($spaceitem),
      $times->{$spaceitem},
      $obj
    )
  

  }

}


Toolkit_helpers::print_output($output, $format, $nohead);


exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_namespace   [ -engine|d <delphix identifier> | -all ] 
                    [ -namespacename name ]
                    [ -configfile file ]
                    [ -format csv|json ]  
                    [ -help|? ] 
                    [ -debug ]

=head1 DESCRIPTION

Get the information about engine replication.

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

=item B<-namespacename name>
Limit output to single namespace

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

List replication jobs, status and schedule

  dx_get_namespace -d dxtest

  Appliance  Namespace            Last complited run   Objects
  ---------- -------------------- -------------------- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- 
  dxtest     localhost-3          2022-10-27 14:00:25  los/maskvdb,Untitled/AdventureWorks2012,Untitled/clonik,Untitled/oratest,Untitled/PDBPIPE,Untitled/PDBPROD,Untitled/test19,Untitled/test19mt
  dxtest     localhost-5          2022-10-27 12:09:08  los/maskvdb,Untitled/PDBPROD,Untitled/test19,Untitled/test19mt
  dxtest     localhost-7          2022-10-27 12:52:36  los/maskvdb,Untitled/test19mt

=cut



