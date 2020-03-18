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
# Program Name : dx_set_key.pl
# Description  : Set key for dSource
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
#
# Copyright (c) 2015 by Delphix. All rights reserved.
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
  'key=s' => \(my $password),
  'name=s' => \(my $dbname),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'envname=s' => \(my $envname),
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

if (!defined($password)) {
  print "Parameter key has to be specified. \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


Toolkit_helpers::check_filer_options (1,'dSource', $group, $host, $dbname, $envname);

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

  my $db_list = Toolkit_helpers::get_dblist_from_filter('dSource', $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {

    my $dbobj = $databases->getDB($dbitem);

    if ( ($dbobj->getDBType() eq 'mssql') || ($dbobj->getDBType() eq 'sybase') ) {
      $ret = $ret + $dbobj->setEncryption($password);
    }

  }

}

exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_set_key  [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
               -group group_name | -name db_name | -host host_name
               <-key password>

 [-help] [-debug]

=head1 DESCRIPTION

Set an encryption password for MS SQL and Sybase dSource specified by filter parameter

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

=item B<-key password>
Specify an encryption password

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


=back


=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Set backup encryption key for database "Sybase dsource"

 dx_set_key -d Landshark51 -name "Sybase dsource" -key "SecurePassword"
 Encryption key for database Sybase dsource set with success.


Remove backup encryption key from database "Sybase dsource"

  dx_set_key -d Landshark51 -name "Sybase dsource" -key ""
  Encryption key for database Sybase dsource set with success.


=cut
