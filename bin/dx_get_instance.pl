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
# Program Name : dx_get_instance.pl
# Description  : Get database and host information
# Author       : Marcin Przepiorowski
# Created      : 30 Nov 2015 (v2.2.1)
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
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'envname=s' => \(my $envname),
  'instance=n' => \(my $instance),
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

Toolkit_helpers::check_filer_options (undef,$type, $group, $host, $dbname, $envname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();


$output->addHeader(
  {'Appliance', 10},
  {'Env name',  20},
  {'Hostname',  30},
  {'Group',     15},
  {'Database',  30},
  {'Instance',  10},
  {'Type',       8},
  {'Status',    10},
);


my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  # filter implementation

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, $envname, $dsource, undef, $instance, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);

    if ($dbobj->getDBType() ne 'oracle') {
      next;
    }

    print Dumper $dbobj->getDBType();
    print Dumper $dbobj->getName();


    my $dbname = $dbobj->getName();
    my $dbtype = $dbobj->getType();

    if (defined($dbobj->getCDBContainerRef())) {
      # database has a CDB container so it's a PDB and there is no instance info
      # dbobj will be switch to container to show data

      #my $contsourceconfig = $databases->{_sourceconfigs}->getSourceConfig($dbobj->getCDBContainerRef());
      #print Dumper $contsourceconfig;
      my $contsource = $databases->{_source}->getSourceByConfig($dbobj->getCDBContainerRef());
      $dbobj = $databases->getDB($contsource->{container});
    }

    print Dumper $dbobj->getInstances();

    if ($dbobj->getInstances() eq 'UNKNOWN') {
      # Oracle is detached, so no information about instances, skip to next
      next;
    }

    for my $inst ( @{$dbobj->getInstances()} ) {
      if (defined($instance) && ($inst->{instanceNumber} ne $instance)) {
        next;
      }

      if (defined($host) && ($dbobj->getInstanceHost($inst->{instanceNumber}) ne $host)) {
        next;
      }
      $output->addLine(
        $engine,
        $dbobj->getEnvironmentName(),
        $dbobj->getInstanceHost($inst->{instanceNumber}),
        $groups->getName($dbobj->getGroup()),
        $dbname,
        $inst->{instanceName},
        $dbtype,
        $dbobj->getInstanceStatus($inst->{instanceNumber})
      );
    }

  }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_instance  [-engine|d <delphix identifier> | -all ]
                  [-group group_name | -name db_name | -host host_name | -type dsource|vdb ]
                  [-format csv|json ]
                  [-help|? ]
                  [-debug ]

=head1 DESCRIPTION

Get the information about Oracle instances.

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

=item B<-type>
Type (dsource|vdb)

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

Display a Oracle instance status

 dx_get_instance -d Landshark43

 Appliance  Env name             Hostname                       Group           Database                       Instance   Type     Status
 ---------- -------------------- ------------------------------ --------------- ------------------------------ ---------- -------- ----------
 Landshark4 racattack            192.168.1.61                   Analytics       racd                           racd1      VDB      down
 Landshark4 racattack            192.168.1.62                   Analytics       racd                           racd2      VDB      down
 Landshark4 LINUXTARGET          172.16.180.251                 Analytics       test1                          test1      VDB      down
 Landshark4 LINUXSOURCE          172.16.180.250                 Sources         Employee Oracle 11G DB         orcl       dSource  up

=cut
