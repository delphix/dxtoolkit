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
# Program Name : dx_upgrade_db.pl
# Description  : Upgrade a DB
# Author       : Marcin Przepiorowski
# Created: 13 Apr 2015 (v2.0.0)
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
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'envinst=s' => \(my $envinst),
  'name=s' => \(my $dbname),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'envname=s' => \(my $envname),
  'reponame=s' => \(my $repositoryname),
  'debug:n' => \(my $debug),
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

if ( ! ( defined($envinst)  ) ) {
  print "Options -envinst is required. \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

Toolkit_helpers::check_filer_options (1,$type, $group, $host, $dbname, $envname);

my $ret = 0;


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);



for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  # filter implementation
  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, $envname, undef, undef, undef, undef, undef, $repositoryname, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);
    $ret = $ret + $dbobj->upgradeVDB($envinst);
  }

}

exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_upgrade_db  [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                < -envinst OracleHome/MSSQLinstance >
                < -group group_name | -name db_name | -host host_name | -type dsource|vdb | -envname name >
                [ -help]
                [ -debug]

=head1 DESCRIPTION

Upgrade a DB specified by filter parameter using home/instance defined in envinst parameter

=head1 ARGUMENTS

=head2 Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

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

=item B<-group name>
Group Name

=item B<-name name>
Database Name

=item B<-host name>
Host Name

=item B<-type type>
Type (dsource|vdb)

=item B<-envname name>
Environment name

=item B<-reponame name>
Filter using reponame

=back


=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Upgrade of MS SQL ( including enabling and disabling VDB)

 dx_ctl_db -d Landshark43 -name autotest -action disable
 Disabling database autotest.
 Starting job JOB-830 for database autotest.
 0 - 5 - 10 - 20 - 30 - 100
 Job JOB-830 finised with state: COMPLETED

 dx_upgrade_db -d Landshark43 -name autotest -envinst MSSQL2012
 Waiting for all actions to complete. Parent action is ACTION-1698
 Upgrade completed with success.


 dx_ctl_db -d Landshark43 -name autotest -action enable
 Enabling database autotest.
 Starting job JOB-831 for database autotest.
 0 - 25 - 75 - 100
 Job JOB-831 finised with state: COMPLETED



=cut
