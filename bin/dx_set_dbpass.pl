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
# Copyright (c) 2016 by Delphix. All rights reserved.
#
# Program Name : dx_set_dbpass.pl
# Description  : Set password for DB Delphix user
# Author       : Marcin Przepiorowski
# Created      : 29 Apr 2016 (v2.2.5)
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
use Formater;
use Toolkit_helpers;
use Databases;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'dbname=s' => \(my $dbname),
  'group=s'  => \(my $group),
  'type=s' => \(my $type),
  'host=s' => \(my $host),
  'username=s' => \(my $username),
  'password=s' => \(my $password),
  'force' => \(my $force),
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


if (! defined($password) )  {
  print "Option password is required. \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

Toolkit_helpers::check_filer_options (1,$type, $group, $host, $dbname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);


my %restore_state;

my $ret = 0;


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

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }


  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);

    if ($dbobj->setCredentials($username, $password, $force)) {
      print "Problem with setting password\n";
      $ret = $ret + 1;
    } else {
      print "Password has been set.\n";
    }

  }


}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_set_dbpass [ -engine|d <delphix identifier> | -all ] [ -configfile file ][ -group group_name | -dbname db_name | -host host_name | -type dsource|vdb ] [-username <username>] -password <password> [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Change user password for a specified database(s)

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script on all engines

=item B<-group>
Group Name

=item B<-dbname>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)

=item B<-username user>
Specify a user

=item B<-password pass>
Specify a password

=back

=head1 OPTIONS

=over 2

=item B<-force>
Skip credential validation. Credentials are verified by update API for all Oracle objects

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Setting new password

 dx_set_dbpass -d Landshark5 -dbname "Oracle dsource" -username delphixdb -password newpass
 Password has been set.

Trying to set a wrong password

 dx_set_dbpass -d Landshark5 -dbname "Oracle dsource" -username delphixdb -password fake
 Password check failed.
 Username or password is invalid.



=cut
