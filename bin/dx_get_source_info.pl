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
# Program Name : dx_get_source_info.pl
# Description  : Get source information
# Author       : Marcin Przepiorowski
# Created      : 05 Oct 2015 (v2.2.0)
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
use Databases;
use Engine;
use Formater;
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name=s' => \(my $dbname),
  'format=s' => \(my $format),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'envname=s' => \(my $envname),
  'debug:i' => \(my $debug),
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

Toolkit_helpers::check_filer_options (undef,'dSource', $group, $host, $dbname, $envname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

$output->addHeader(
  {'Appliance', 10},
  {'Database',  30},
  {'Group',     15},
  {'Data Source', 30},
  {'Log Sync',  10},
  {'BCT',       10},
);



my %save_state;

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  # filter implementation

  my $db_list = Toolkit_helpers::get_dblist_from_filter('dSource', $group, $host, $dbname, $databases, $groups, $envname, undef, 'primary', undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);

    $output->addLine(
      $engine,
      $dbobj->getName(),
      $groups->getName($dbobj->getGroup()),
      $dbobj->getSourceConfigName(),
      $dbobj->getLogSync(),
      $dbobj->getBCT()
    );


  }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_source_info    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                       [ -group group_name | -name db_name | -host host_name ]
                       [ -format csv|json ]
                       [ -help|? ]
                       [ -debug ]

=head1 DESCRIPTION

Get the information about source databases.

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

=head2 Filters

Filter databases using one of the following filters

=over 4

=item B<-group>
Group Name

=item B<-name>
Database Name

=item B<-host>
Host Name

=item B<-envname>
Environment name

=back

=head1 OPTIONS

=over 3


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

Display information about data sources

 dx_get_source_info -d Landshark5

 Appliance  Database                       Group           Data Source                    Log Sync   BCT
 ---------- ------------------------------ --------------- ------------------------------ ---------- ----------
 Landshark5 AdventureWorksLT2008R2         Sources         AdventureWorksLT2008R2         INACTIVE   N/A
 Landshark5 Oracle dsource                 Sources         orcl                           INACTIVE   UNKNOWN
 Landshark5 PDB                            Sources         PDB                            ACTIVE     UNKNOWN
 Landshark5 RMAN dsource                   Sources         rmantest                       INACTIVE   UNKNOWN
 Landshark5 Swingbench                     Sources         Swingbench dir                 INACTIVE   N/A
 Landshark5 Sybase dsource                 Sources         pubs3                          INACTIVE   N/A
 Landshark5 orcl_tar                       Sources         orcl_tar                       INACTIVE   UNKNOWN
 Landshark5 racdba                         Sources         racdba                         INACTIVE   UNKNOWN
 Landshark5 singpdb                        Sources         singpdb                        ACTIVE     DISABLED


=cut
