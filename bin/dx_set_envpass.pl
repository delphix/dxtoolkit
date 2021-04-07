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
# Copyright (c) 2016,2019 by Delphix. All rights reserved.
#
# Program Name : dx_set_envpass.pl
# Description  : Set password for OS Delphix user or DB Delphix user
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
use Environment_obj;
use Toolkit_helpers;
use Host_obj;
use Action_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'envname=s' => \(my $envname),
  'username=s' => \(my $username),
  'password=s' => \(my $password),
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


if (! (defined($username) && defined($password) ) ) {
  print "Option username and password are required. \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

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
  my $environments = new Environment_obj( $engine_obj, $debug);
  my $hosts = new Host_obj ( $engine_obj, $debug );

  # filter implementation

  my @env_list;

  if (defined($envname)) {
    push(@env_list, $environments->getEnvironmentByName($envname)->{reference});
  } else {
    @env_list = $environments->getAllEnvironments();
  };

  # for filtered databases on current engine - display status
  for my $envitem ( @env_list ) {

    my $env_user = $environments->getEnvironmentUserByName($envitem, $username);

    my $envname = $environments->getName($envitem);

    if (!defined($env_user)) {
      print "User $username not found in environment $envname . \n";
      next;
    }

    my $hostref = $environments->getHost($envitem);

    my $connfault = 0;
    my $result;


    if ( $environments->getType($envitem) ne 'windows' ) {

      # check ssh connectivity



      if ( $hostref eq 'CLUSTER' ) {
        my $cluhosts =  $environments->getOracleClusterNode($envitem);
        for my $clunode ( @{$cluhosts} ) {

          my $nodeaddr = $hosts->getHostAddr($clunode->{host});
          my $port = $hosts->getHostPort($clunode->{host});
          ($connfault, $result)  = $connfault + $engine_obj->checkSSHconnectivity($username, $password, $nodeaddr, $port);

        }
      } else {
        my $nodeaddr = $hosts->getHostAddr($hostref);
        my $port = $hosts->getHostPort($hostref);
        ($connfault, $result) = $engine_obj->checkSSHconnectivity($username, $password, $nodeaddr, $port);
      }

    } else {
      if ( $hostref ne 'CLUSTER' ) {
        my $nodeaddr = $hosts->getHostAddr($hostref);
        ($connfault, $result) =  $engine_obj->checkConnectorconnectivity($username, $password, $nodeaddr);
      }


    }

    my $jobno;

    if ($connfault) {
      print "Error. Provided credentials doesn't work for environment $envname.\n";
      $ret = $ret + 1;
    } else {
      $jobno = $environments->changePassword($env_user, $password);
    }

    $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Password change actions is completed with success for environment $envname.", "There were problems with changing password.");

  }


}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_set_envpass  [ -engine|d <delphix identifier> | -all ] [ -configfile file ][ -envname env_name ] -username <username> -password <password> [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Change user password for an environment

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script on all engines

=item B<-envname name>
Specify an environment name

=item B<-username user>
Specify a user

=item B<-password pass>
Specify a password

=back

=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Set a new environment password for environment LINUXSOURCE

 dx_set_envpass -d Landshark5 -envname LINUXSOURCE -username delphix -password delphix
 Waiting for all actions to complete. Parent action is ACTION-5877
 Password change actions is completed with success for environment LINUXSOURCE.



=cut
