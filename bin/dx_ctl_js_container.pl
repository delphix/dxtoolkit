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
# Program Name : dx_ctl_js_container.pl
# Description  : Get Delphix Engine JS container
# Author       : Marcin Przepiorowski
# Created      : 02 Mar 2016 (v2.2.5)
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
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'debug:i' => \(my $debug)
) or pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);

pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);
}


if (!defined($container_name)) {
  print "Container name is required \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);  
}

if (!defined($action) || ( ! ( (lc $action eq 'reset' ) || (lc $action eq 'refresh' ) || (lc $action eq 'recover' ) ) ) ) {
  print "Action parameter not specified or has a wrong value - $action \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
  exit (1);  
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };
  
  my $jstemplates = new JS_template_obj ($engine_obj, $debug );

  my $template_ref;

  if (defined($template_name)) {
    my $template_ref = $jstemplates->getJSTemplateByName($template_name);
    if (! defined($template_ref)) {
      print "Can't find template $template_name \n";
      $ret = $ret + 1;
      next;
    }
  }


  my $jscontainers = new JS_container_obj ( $engine_obj, $template_ref, $debug);
  my $jscontainer_ref = $jscontainers->getJSContainerByName($container_name);
  if (! defined($jscontainer_ref)) {
    print "Can't find container name $container_name\n";
    next;
  }
  
  my $jobno;

  if (lc $action eq 'reset') {
    $jobno = $jscontainers->resetContainer($jscontainer_ref);
  } elsif (lc $action eq 'refresh') {
    $jobno = $jscontainers->refreshContainer($jscontainer_ref);
  } elsif (lc $action eq 'recover') {
    $jobno = $jscontainers->recoverContainer($jscontainer_ref, $timestamp);
  }



  if (defined ($jobno) ) {
    print "Starting job $jobno for container $container_name.\n";
    my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);

    my $jobstat = $job->waitForJob();
    if ($jobstat ne 'COMPLETED') {
      $ret = $ret + 1;
    }
  } else {
    print "Job for container is not created. \n";
    $ret = $ret + 1;
  }

}


exit $ret;



__DATA__

=head1 SYNOPSIS

dx_ctl_js_container    [ -engine|d <delphix identifier> | -all ] -action reset|refresh|recover -container_name container_name 
                       [-template_name template_name]  
                       [-timestamp timestamp]
                       [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Run a action on the JetStream container

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-action reset|refresh|recover>
Run a action on the container

- reset - reset JS container to latest point
- refresh - refresh JS container from latest point of template
- recover - recover a JS container to point in time or bookmark

=item B<-container_name container_name>
Name of container to run action on 

=item B<-template_name template_name>
Name of container's templates

=back

=head1 OPTIONS

=over 3

=item B<-timestamp "YYYY-MM-DD HH24:MI:SS" or bookmark name >                                                                                                                                            
Use timestamp or bookmark name to recover container

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=back




=cut



