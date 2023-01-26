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
# Program Name : dx_ctl_namespace.pl
# Description  : Control namespaces
# Author       : Marcin Przepiorowski
# Created      : Nov 2022 
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
use Replication_obj;
use Formater;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

my $smart = 'yes';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'namespacename=s' => \(my $namespacename),
  'action=s' => \(my $action),
  'skip' => \(my $skip),
  'smart=s' => \($smart),
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

if (!defined($namespacename)) {
  print "Namespace name is mandatory. Please specify -namespacename parameter \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
  };

  my $namespaces = new Namespace_obj( $engine_obj, $debug );

  my $ref = $namespaces->getNamespaceByName($namespacename);
  if (!defined($ref)) {
    print "Replicated namespace with name $namespacename doesn't exist\n";
    $ret = $ret + 1;
    next;
  }

  print "Going to $action namespace - $namespacename\n";

  if (!defined ($skip)) {

    print "Are you sure (y/(n)) - use -skip to skip this confirmation \n";

    my $ok = <STDIN>;

    chomp $ok;

    if (($ok eq '') || (lc $ok ne 'y')) {
      print "Exiting.\n";
      exit(1);
    }

  }

  if (lc $action eq 'failover') {

    my $jobno = $namespaces->failovernamespace($ref, $smart);

    if (defined($jobno)) {
      print "Failing over replicated namespace $namespacename\n";
      $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Namespace failed over","Problem with namespace fail over");
    } else {
      print "Problem with defining a namespace deletion. Please run with -debug\n";
    }

  } elsif (lc $action eq 'delete') {

    my $jobno = $namespaces->deletenamespace($ref);

    if (defined($jobno)) {
      print "Deleting replicated namespace $namespacename\n";
      $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Namespace deleted","Problem with namespace deletion");
    } else {
      print "Problem with defining a namespace deletion. Please run with -debug\n";
    }

  } else {
    print "Unknown action\n";
    $ret = $ret + 1;
    next;
  }

}



exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_namespace   [-engine|d <delphix identifier> | -all ]
                    -namespacename namespace
                    -action delete|failover
                    [-skip]
                    [-smart yes|no]
                    [-help|?]
                    [-debug ]

=head1 DESCRIPTION

Control a replicated namespace - delete or failover

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

=item B<-namespacename namespace>
Specify a namespace name for action

=item B<-action delete|failover>
Specify an action to run with namespace name

- delete - to delete an existing namespace

- failover - to failover an existing namespace

=back


=head1 OPTIONS

=over 3

=item B<-skip>
To skip confirmation - handle with care

=item B<-smart yes|no>
Use Delphix smart failover 

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 Example

Failover namespace

  dx_ctl_namespace -d replica -action failover -namespacename ip-10-110-215-98-1
  Going to failover namespace - ip-10-110-215-98-1
  Are you sure (y/(n)) - use -skip to skip this confirmation
  y
  Failing over replicated namespace ip-10-110-215-98-1
  Waiting for all actions to complete. Parent action is ACTION-11
  Namespace failed over

Failover namespace skipping confirmation and using a smart failover option

  dx_ctl_namespace -d replica -action failover -namespacename ip-10-110-215-98-3 -skip -smart yes
  Going to failover namespace - ip-10-110-215-98-3
  Failing over replicated namespace ip-10-110-215-98-3
  Waiting for all actions to complete. Parent action is ACTION-30
  Namespace failed over

Deleting namespace

  dx_ctl_namespace -d replica -action delete -namespacename ip-10-110-215-98-3
  Going to delete namespace - ip-10-110-215-98-3
  Are you sure (y/(n)) - use -skip to skip this confirmation
  y
  Deleting replicated namespace ip-10-110-215-98-3
  Waiting for all actions to complete. Parent action is ACTION-31
  Namespace deleted

=cut
