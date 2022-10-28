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
# Program Name : dx_ctl_replication.pl
# Description  : Get information about replication
# Author       : Marcin Przepiorowski
# Created      : 28 Sept 2016 (v2.2.0)
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
  'nowait' => \(my $nowait),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'safe' => \(my $safe),
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

 dx_ctl_replication [-engine|d <delphix identifier> | -all ]
                    -profilename profile
                    -action create|delete|update|replicate
                    [-enabled yes|no]
                    [-schedule "* * * * *"]
                    [-objects "Groupname/dbname"[,"Groupname"]]
                    [-host hostname]
                    [-user username]
                    [-password password]
                    [-safe]
                    [-nowait]
                    [-help|?]
                    [-debug ]

=head1 DESCRIPTION

Start an replication using a profile name

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

=item B<-profilename profile>
Specify a profile name to run

=item B<-action create|delete|update|replicate>
Specify an action to run with profile name

- create - to create a new profile

- delete - to delete an existing profile

- update - to update an existing profile

- replicate - to kick off replication

=back


=head1 OPTIONS

=over 3

=item B<-enabled yes|no>
Enable automatic replication

=item B<-schedule "* * * * *">
Replication schedule using Quartz-cron expression

Ex:
"0 0 */4 ? * *" - run every 4 hour

=item B<-objects "Groupname/dbname"[,"Groupname"]>
Comma separated list of objects [ dataset / group ] to replicate.
Database has to be provided with a group name.

Ex:
PRD,TEST/test19 - group PRD and dataset test19 from group TEST will be added to replication

=item B<-host hostname>
Replica engine hostname / IP

=item B<-user username>
Replica engine username

=item B<-password password>
Replica engine password

=item B<-nowait>
Don't wait for a replication job to complete. Job will be running in background.

=item B<-safe>
Enable "safe" replication. If there was a VDB/dSource deletion operation
on primary engine, replication job won't be started

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 Example

Replicate a profile called "backup"

  dx_ctl_replication -d DelphixEngine -profilename backup -nowait
  Replication job JOB-7425 started in background

Create a replica profile called newprof replicating group PRD and one VDB (test19) from group TEST every 2 hours

  dx_ctl_replication -d DE -action create -profilename newprof -objects PRD,TEST/test19 -host 10.0.0.1 -user admin -password xxxxxxxx -enabled yes -type replica  -schedule "0 0 */2 ? * *"

Update an existing replica profile

  dx_ctl_replication -d DE -action update -profilename newprof -enabled no

Delete an existing replica profile

  dx_ctl_replication -d DE -action delete -profilename newprof 

=cut
