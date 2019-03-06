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
# Program Name : dx_get_js_branches.pl
# Description  : Get Delphix Engine JS branches
# Author       : Marcin Przepiorowski
# Created      : Sept 2018 (v2.3.6)
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
use Users;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'template_name=s' => \(my $template_name),
  'container_name=s' => \(my $container_name),
  'branch_name=s' => \(my $branch_name),
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


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);
my $output = new Formater();


$output->addHeader(
    {'Appliance'     , 20},
    {'Container name', 20},
    {'Template name' , 20},
    {'Branch name'   , 20},
    {'Full name'     , 30}
);
# }

my $ret = 0;


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };


  my $jstemplates = new JS_template_obj ($engine_obj, $debug );

  my $jsbranches;
  my $template_ref;

  if (defined($template_name)) {
    $template_ref = $jstemplates->getJSTemplateByName($template_name);
    if (! defined($template_ref)) {
      print "Can't find template $template_name \n";
      $ret = $ret + 1;
      next;
    }

  }

  my $jscontainers = new JS_container_obj ( $engine_obj, $template_ref, $debug);

  my @branchlist;

  if (defined($template_name) || defined($container_name)) {
    # only branches from template or container need to be loaded
    my $datasource_ref;
    if (defined($container_name)) {
      # load all branches from container name
      my @contarr = @{$jscontainers->getJSContainerByName($container_name, 1)};
      if (scalar(@contarr)<1) {
        print "Can't find container $container_name \n";
        $ret = $ret + 1;
      }
      for my $cont (@contarr) {
        if (!defined($jsbranches)) {
          $jsbranches = new JS_branch_obj ( $engine_obj, $cont, $debug );
        } else {
          $jsbranches->loadJSBranchList($cont);
        }
      }
    } else {
      # load all branches from template
      $jsbranches = new JS_branch_obj ( $engine_obj, $template_ref, $debug );
    }
  } else {
    # all branches are loaded
    $jsbranches = new JS_branch_obj ( $engine_obj, undef, $debug );
  }

  my $dataobj;
  my $printtemplate;
  my $printcontainer;

  if (defined($branch_name)) {
    @branchlist = @{$jsbranches->getJSBranchByName($branch_name, 1)};
    if (scalar(@branchlist) < 1) {
      $ret = $ret + 1;
    }
  } else {
    @branchlist = @{$jsbranches->getJSBranchList()};
  }

  my $fullname;

  for my $branch (@branchlist) {
    $dataobj = $jsbranches->getDataobj($branch);
    $printtemplate = $jstemplates->getName($dataobj);
    if ($printtemplate eq 'N/A') {
      # it's not template
      $printcontainer = $jscontainers->getName($dataobj);
      my $tempref =$jscontainers->getJSContainerTemplate($dataobj);
      $printtemplate = $jstemplates->getName($tempref);
      $fullname = $printcontainer . '/' . $jsbranches->getName($branch);
    } else {
      # it's template
      $printcontainer = 'N/A';
      $fullname = $printtemplate . '/' . $jsbranches->getName($branch);
    }
    $output->addLine(
      $engine,
      $printcontainer,
      $printtemplate,
      $jsbranches->getName($branch),
      $fullname
    );
  }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_js_branches     [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                        [ -template_name template_name ]
                        [ -container_name container_name ]
                        [ -branch_name branch_name ]
                        [ -format csv|json ]
                        [ --help|? ]
                        [ -debug ]

=head1 DESCRIPTION

Get the list of Jet Stream containers from Delphix Engine.

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

=back

=head2 Options

=over 4

=item B<-template_name template_name>
If used without container_name this option will display branch for template_name
If used with container_name this option will limit containers to specific template

=item B<-container_name container_name>
Display branches for particular container_name

=item B<-branch_name branch_name>
Display branches for particular branch_name

=back

=head1 OPTIONS

=over 3

=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-nohead>
Turn off header output

=back

=head1 EXAMPLES

List all branches

 dx_get_js_branches -d Landshark5

 Appliance            Container name       Template name        Branch name
 -------------------- -------------------- -------------------- --------------------
 Landshark5           N/A                  testdx               master
 Landshark5           N/A                  other                master
 Landshark5           testcon              other                default
 Landshark5           testcon              testdx               now
 Landshark5           testcon              testdx               frombook

List branches for container name

 dx_get_js_branches -d Landshark5 -container_name testcon

 Appliance            Container name       Template name        Branch name
 -------------------- -------------------- -------------------- --------------------
 Landshark5           testcon              other                default
 Landshark5           testcon              testdx               ala
 Landshark5           testcon              testdx               frombook

=cut
