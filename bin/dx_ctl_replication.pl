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

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'profilename=s' => \(my $profilename),
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

if (!defined($profilename)) {
  print "Profile name is mandatory. Please specify -profilename parameter \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    #print "Can't connect to Dephix Engine $engine\n\n";
    next;
  };

  my $replication = new Replication_obj( $engine_obj, $debug );

  my $ref = $replication->getReplicationByName($profilename);
  if (!defined($ref)) {
    $ret = $ret + 1;
    next;
  }
  my $jobno;
  if (defined($ref)) {

    if (defined($safe)) {
      my $lastjob = $replication->getLastJob($ref, 1);
      my $actions = new Action_obj($engine_obj, $lastjob->{StartTime}, undef, undef, undef, $debug);
      $actions->loadActionList();
      my @deletelist = @{$actions->getActionList('asc', 'DB_DELETE', undef)};

      if (scalar(@deletelist)>0) {
        print "There was a delete database operation on primary engine. List of databases:\n";
        my $name;
        for my $ar (@deletelist) {
          $name = $actions->getDetails($ar);
          $name =~ s/Delete dataset//;
          $name =~ s/"//g;
          $name =~ s/\.//;
          print Toolkit_helpers::trim($name) . "\n";
        }
        print "Replication won't be kicked off\n";
        $ret = $ret + 1;
        next;
      }

    }

    $jobno = $replication->replicate($ref);
    if (defined($nowait)) {
      if (defined($jobno)) {
        print "Replication job $jobno started in background\n";
      } else {
        print "Problem with defining a replication job. Please run with -debug\n";
      }
    } else {
      if (defined($jobno)) {
        print "Starting replication job $jobno\n";
        $ret = $ret + Toolkit_helpers::waitForJob($engine_obj, $jobno, "Replication job finished","Problem with replication");
      } else {
        print "Problem with defining a replication job. Please run with -debug\n";
      }
    }
  }





}


exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_replication [-engine|d <delphix identifier> | -all ]
                     -profilename profile
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

=back

=head1 OPTIONS

=over 3

=item B<-nowait>
Don't wait for a replication job to complete. Job will be running in backgroud.

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

  dx_get_replication -d DelphixEngine -profilename backup -nowait
  Replication job JOB-7425 started in background

=cut
