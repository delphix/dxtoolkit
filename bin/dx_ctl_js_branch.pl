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
# Copyright (c) 2018 by Delphix. All rights reserved.
#
# Program Name : dx_ctl_js_branch.pl
# Description  : Control JS branches
# Author       : Marcin Przepiorowski
# Created      : Sept 2018 (v2.3.7)
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
use JS_template_obj;
use JS_container_obj;
use JS_branch_obj;
use Jobs_obj;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'container_name=s' => \(my $container_name),
  'template_name=s' => \(my $template_name),
  'timestamp=s' => \(my $timestamp),
  'action=s' => \(my $action),
  'branch_name=s' => \(my $branch_name),
  'from_branch=s' => \(my $from_branch),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
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




if (!defined($action) || ( ! ( (lc $action eq 'create' ) || (lc $action eq 'delete' ) || (lc $action eq 'activate' )  ) ) ) {
  print "Action parameter not specified or has a wrong value\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (!defined($container_name)) {
  print "Container name is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (!defined($branch_name)) {
  print "Branch name is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };


  my $jstemplates = new JS_template_obj ($engine_obj, $debug );

  my $dataobj_ref;
  my $template_ref;

  if (defined($template_name)) {
    $template_ref = $jstemplates->getJSTemplateByName($template_name);
    if (! defined($template_ref)) {
      print "Can't find template $template_name \n";
      $ret = $ret + 1;
      next;
    }
  }

  my $jobno;

  my $jscontainers = new JS_container_obj ( $engine_obj, $template_ref, $debug);

  my $jscontainer_ref = $jscontainers->getJSContainerByName($container_name);
  if (! defined($jscontainer_ref)) {
    $ret = $ret + 1;
    next;
  }

  my $jsbranches = new JS_branch_obj ( $engine_obj, $jscontainer_ref, $debug );

  if (lc $action eq 'create') {
    $jobno = $jsbranches->createBranch($jscontainers, $branch_name, $jscontainer_ref, $timestamp, $from_branch);
  } elsif (lc $action eq 'delete') {
    my $branch_ref = $jsbranches->getJSBranchByName($branch_name);
    if (defined($branch_ref)) {
      $jobno = $jsbranches->deleteBranch($branch_ref);
    } else {
      print "Branch name not found\n";
      $ret = $ret + 1;
      next;
    }
  } elsif (lc $action eq 'activate') {
    my $branch_ref = $jsbranches->getJSBranchByName($branch_name);
    if (defined($branch_ref)) {
      $jobno = $jsbranches->activateBranch($branch_ref);
    } else {
      print "Branch name not found\n";
      $ret = $ret + 1;
      next;
    }
  }

  if (defined($jobno)) {
    Toolkit_helpers::waitForJob(
      $engine_obj,
      $jobno,
      "Job for branch $branch_name completed",
      "Problem with job for brach $branch_name");
  } else {
    print "Problem with defining branch job\n";
    $ret = $ret + 1;
  }

}


exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_ctl_js_branch       [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                        -action create|delete|activate
                        -container_name container_name
                        -branch_name branch_name
                        [ -template_name template_name ]
                        [ -timestamp timestamp ]
                        [ -from_branch branch_name ]
                        [ -help|? ]
                        [ -debug ]

=head1 DESCRIPTION

Run a action on the JetStream branch

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 3

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

=item B<-action create|delete|activate>
Run a action on the branch

=over 3

=item B<-create> - create branch

=item B<-delete> - delete branch

=item B<-activate> - activate branch

=back

=item B<-container_name container_name>
Name of container to run action on

=item B<-template_name template_name>
Name of container's templates

=back

=head1 OPTIONS

=over 3

=item B<-timestamp "YYYY-MM-DD HH24:MI:SS" or bookmark name >
Use timestamp or bookmark name to create branch.
If timestamp options is not specified, branch will be created from latest point

=item B<-from_branch branch_name >
Create branch from particular branch.
If from_branch option is not specified an active branch will be used

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 Examples

Create branch from active branch using latest point in time

 dx_ctl_js_branch -d Landshark5 -action create -container_name testcon -template_name testdx -branch_name latest_branch
 Starting job - JOB-9755
 0 - 1 - 5 - 13 - 24 - 30 - 37 - 42 - 46 - 50 - 58 - 76 - 77 - 80 - 95 - 100
 Job JOB-9755 finished with state: COMPLETED
 Job for branch latest_branch completed.

Create branch using a bookmark name

 dx_ctl_js_branch -d Landshark5 -action create -container_name testcon -branch_name frombook -timestamp bookmark12
 Starting job - JOB-9759
 0 - 1 - 5 - 13 - 24 - 30 - 37 - 42 - 46 - 50 - 58 - 76 - 77 - 80 - 95 - 100
 Job JOB-9755 finished with state: COMPLETED
 Job for branch frombook completed.

Create branch using a timestamp

 dx_ctl_js_branch -d Landshark5 -action create -container_name testcon -branch_name fromtime -timestampe "2018-09-19 10:10:12"
 Starting job - JOB-9760
 0 - 1 - 5 - 13 - 24 - 30 - 37 - 42 - 46 - 50 - 58 - 76 - 77 - 80 - 95 - 100
 Job JOB-9755 finished with state: COMPLETED
 Job for branch fromtime completed.

Delete branch

 dx_ctl_js_branch -d Landshark5 -action delete -container_name testcon -template_name testdx -branch_name new_branch
 Starting job - JOB-9754
 0 - 100
 Job JOB-9754 finished with state: COMPLETED
 Job for branch new_branch completed.

Activate branch

 dx_ctl_js_branch -d Landshark5 -action activate -container_name testcon -branch_name new_branch
 Starting job - JOB-9746
 5 - 10 - 72 - 92 - 94 - 100
 Job JOB-9746 finished with state: COMPLETED
 Job for branch new_branch completed.

Activate branch using container and template name

 dx_ctl_js_branch -d Landshark5 -action activate -container_name testcon -template_name testdx -branch_name now_branch
 Starting job - JOB-9750
 5 - 10 - 72 - 92 - 94 - 100
 Job JOB-9746 finished with state: COMPLETED
 Job for branch now_branch completed.

=cut
