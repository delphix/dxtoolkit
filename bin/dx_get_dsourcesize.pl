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
# Copyright (c) 2015,2018 by Delphix. All rights reserved.
#
# Program Name : dx_get_dsourcesize.pl
# Description  : Get dSource size for ingestion model
# Author       : Marcin Przepiorowski
# Created      : September 2018
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
  'license' => \(my $license),
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

Toolkit_helpers::check_filer_options (undef,undef, $group, $host, $dbname, $envname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

if ($license) {
  $output->addHeader(
    {'Appliance', 10},
    {'Type',      40},
    {'Database',  40},
    {'Size [GB]', 30}
  );
} else {
  $output->addHeader(
    {'Appliance', 10},
    {'Env name',  20},
    {'Group',     15},
    {'Database',  30},
    {'Size [GB]', 30},
    {"Status",    30},
    {"Enabled",   30}
  );
}


my $ret = 0;

print "# Delphix can automatically calculate the usage for Oracle, SQL Server and ASE databases for each Delphix Engine.\n";
print "# For other databases, and before the source is connected to the Delphix Engine\n";
print "# you will need to run a query on the source system for the relevant data.\n";

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
  };

  if ($license) {
    # use licence API
    if (version->parse($engine_obj->getApi()) >= version->parse(1.10.3)) {
      my $lic = $engine_obj->getLicenseUsage();
      if (defined($lic->{"databases"})) {
        for my $db ( @{$lic->{"databases"}}) {
          $output->addLine(
            $engine,
            $db->{"type"},
            $db->{"name"},
            sprintf("%10.2f", $db->{"size"}/1024/1024/1024)
          )
        }
      }
    } else {
      print "There is no license API. Results returned by non license API as using method described in CLI method in the Delphix Pricing Guide.\n";
      print "For details please contact your Delphix account manager\n";
      exit(1);
    }

  } else {

    # load objects for current engine
    my $databases = new Databases( $engine_obj, $debug);
    my $groups = new Group_obj($engine_obj, $debug);

    # filter implementation

    my $db_list = Toolkit_helpers::get_dblist_from_filter('dSource', $group, $host, $dbname, $databases, $groups, $envname, undef, undef, undef, undef, undef, undef, $debug);
    if (! defined($db_list)) {
      print "There is no DB selected to process on $engine . Please check filter definitions. \n";
      $ret = $ret + 1;
      next;
    }

    # for filtered databases on current engine - display status
    for my $dbitem ( @{$db_list} ) {
      my $dbobj = $databases->getDB($dbitem);

      $output->addLine(
        $engine,
        $dbobj->getEnvironmentName(),
        $groups->getName($dbobj->getGroup()),
        $dbobj->getName(),
        $dbobj->getRuntimeSize(),
        $dbobj->getRuntimeStatus(),
        $dbobj->getEnabled()
      );


    }

  }



}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_dsourcesize  [-engine|d <delphix identifier> | -all ]
                     [-group group_name | -name db_name | -host host_name | -envname env_name ]
                     [-format csv|json ]
                     [-help|? ]
                     [-debug ]

=head1 DESCRIPTION

Get the information about dSource sizes. If you want to use those data for ingestion model reporting
please use -license option of the script.

Delphix can automatically calculate the usage for Oracle, SQL Server and ASE databases for each Delphix Engine.
For other databases, and before the source is connected to the Delphix Engine,
you will need to run a query on the source system for the relevant data.

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

Display dSources sizes from engine

 dx_get_dsourcesize.pl -d Landshark5

 Appliance  Env name             Group           Database                       Size [GB]                      Status                         Enabled
 ---------- -------------------- --------------- ------------------------------ ------------------------------ ------------------------------ ------------------------------
 Landshark5 windows2012source    Sources         AdventureWorks2012             0.00                           UNKNOWN                        disabled
 Landshark5 LINUXSOURCE          Sources         Oracle 121                     6.46                           UNKNOWN                        disabled
 Landshark5 LINUXSOURCE          Sources         Oracle 122                     7.30                           UNKNOWN                        disabled
 Landshark5 LINUXSOURCE          Sources         test121                        7.49                           RUNNING                        enabled

Display dSources sizes from all configured engines in csv format

  dx_get_dsourcesize.pl -all -format csv
  #Appliance,Env name,Group,Database,Size [GB],Status,Enabled
  Landshark5,windows2012source,Sources,AdventureWorks2012,0.00,UNKNOWN,disabled
  Landshark5,LINUXSOURCE,Sources,Oracle 121,6.46,UNKNOWN,disabled
  Landshark5,LINUXSOURCE,Sources,Oracle 122,7.30,UNKNOWN,disabled
  Landshark5,LINUXSOURCE,Sources,test121,7.49,RUNNING,enabled


=cut
