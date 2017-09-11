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
# Program Name : dx_ctl_jobs.pl
# Description  : Control jobs
# Author       : Marcin Przepiorowski
# Created      : 04 Jan 2016 (v2.2.0)
#

use warnings;
use strict;
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
use Jobs;
use User_obj;

my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'st=s' => \(my $st), 
  'et=s' => \(my $et), 
  'state=s' => \(my $state),
  'jobref=s'   => \(my $jobref),
  'action=s'   => \(my $action),
  'format=s' => \(my $format), 
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug),
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



if (defined($state) && ( ! ( (uc $state eq 'COMPLETED') || (uc $state eq 'FAILED') || (uc $state eq 'RUNNING') || (uc $state eq 'SUSPENDED') || (uc $state eq 'CANCELED')  ) ) ) {
  print "Option state can have only COMPLETED, WAITING and FAILED value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if (! defined($action)) {
  print "Action is required\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( ( ! ( (uc $action eq 'CANCEL') || (uc $action eq 'RESUME') || (uc $action eq 'SUSPEND')  ) ) ) {
  print "Argument action can have only CANCEL, RESUME and SUSPEND value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

my $ret = 0;

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  my $st_timestamp;
  my $et_timestamp;


  if (! defined($st_timestamp = Toolkit_helpers::timestamp($st, $engine_obj))) {
      print "Wrong start time (st) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);  
  }

  if (defined($et)) {
    $et = Toolkit_helpers::timestamp_to_timestamp_with_de_timezone($et, $engine_obj);
    if (! defined($et_timestamp = Toolkit_helpers::timestamp($et, $engine_obj))) {
      print "Wrong end time (et) format \n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);  
    } 
  }

  my $jobs = new Jobs($engine_obj, $st_timestamp, $et_timestamp, $state, undef, undef, $jobref, undef, undef, undef, $debug);

  my @jobsarr;
  @jobsarr = @{$jobs->getJobList('asc')};




  for my $jobitem ( @jobsarr ) {

    my $jobobj = $jobs->getJob($jobitem);

    if (uc $action eq 'CANCEL') {
      if ($jobobj->cancel() ) {
        print "Error while canceling job - $jobitem\n";
        $ret = $ret + 1;
      } else {
        print "Job - $jobitem - cancelled\n"
      }
    } elsif (uc $action eq 'SUSPEND') {
      if ($jobobj->suspend() ) {
        print "Error while suspending job - $jobitem\n";
        $ret = $ret + 1;
      } else {
        print "Job - $jobitem - suspended\n"
      }
    } elsif (uc $action eq 'RESUME') {
      if ($jobobj->resume() ) {
        print "Error while resuming job - $jobitem\n";
        $ret = $ret + 1;
      } else {
        print "Job - $jobitem - resumed\n"
      }
    } 


  }
}


exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_jobs    [ -engine|d <delphix identifier> | -all ] 
                -action CANCEL|SUSPEND|RESUME 
                [-jobref ref] 
                [-st timestamp] 
                [-et timestamp] 
                [-state state] 
                [-help|? ] 
                [-debug ]

=head1 DESCRIPTION

Run an action for a list of jobs from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

Filter faults using one of the following filters

=over 4

=item B<-state>
Job state - COMPLETED / FAILED / RUNNING / SUSPENDED / CANCELED

=item B<-jobref ref>
Job reference


=back

=head1 OPTIONS

=over 3

=item B<-action actionname>
Run a particular action for a list of jobs - CANCEL | RESUME | SUSPEND 


=item B<-st timestamp>
Start time for faults list - default value is 7 days

=item B<-et timestamp>
End time for faults list 


=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 EXAMPLES

Cancel a job JOB-267199

 dx_ctl_jobs -d Delphix32 -action cancel -jobref JOB-267199 
 Job - JOB-267199 - cancelled

=cut



