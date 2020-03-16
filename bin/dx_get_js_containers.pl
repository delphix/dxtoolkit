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
# Copyright (c) 2016,2018 by Delphix. All rights reserved.
#
# Program Name : dx_get_js_containers.pl
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
use Users;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'template_name=s' => \(my $template_name),
  'container_name=s' => \(my $container_name),
  'listdb' => \(my $listdb),
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

if (defined($listdb)) {
  $output->addHeader(
      {'Appliance'     , 20},
      {'Container name', 20},
      {'Template name' , 20},
      {'Active branch' , 20},
      {'Owners'        , 50},
      {'Database name' , 50}
  );
}
else {
  $output->addHeader(
      {'Appliance'     , 20},
      {'Container name', 20},
      {'Template name' , 20},
      {'Active branch' , 20},
      {'Owners'        , 50}
  );
}
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

  my $databases;
  my $groups;
  if (defined($listdb)) {
    $databases = new Databases ( $engine_obj , $debug);
    $groups = new Group_obj($engine_obj, $debug);
  }

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
  my $jsbranches = new JS_branch_obj ( $engine_obj, $template_ref, $debug );

  my @contarr;

  if (defined($container_name)) {
    @contarr = @{$jscontainers->getJSContainerByName($container_name, 1)};
    if (scalar(@contarr)<1) {
      print "Can't find container $container_name \n";
      $ret = $ret + 1;
    }
  } else {
    @contarr = @{$jscontainers->getJSContainerList()};
  }

  my $users = new Users ( $engine_obj, $debug);

  for my $jsconitem (@contarr) {

    my @owners_array = ();
    my $owners = $users->getUsersByTarget($jsconitem);
    if (defined($owners)) {
      for my $owner (@{$owners}) {
        my $userobj = $users->getUser($owner);
        if (defined($userobj)) {
          push(@owners_array, $userobj->getName());
        }
      }
    }

    my $owners_string = join(';',@owners_array);


    if (defined($listdb)) {
      my $jsdatasources = new JS_datasource_obj ( $engine_obj , $jsconitem, undef);
      my $display_db_name = "";
      for my $ds (@{$jsdatasources->getJSDataSourceList()}) {
          $display_db_name = $groups->getName($databases->getDB($jsdatasources->getJSDBContainer($ds))->getGroup()). " / " . $databases->getDB($jsdatasources->getJSDBContainer($ds))->getName() ;
      }
      $output->addLine(
         $engine,
         $jscontainers->getName($jsconitem),
         $jstemplates->getName($jscontainers->getJSContainerTemplate($jsconitem)),
         $jsbranches->getName($jscontainers->getJSActiveBranch($jsconitem)),
         $owners_string,
         $display_db_name
      );
    } else {
      $output->addLine(
         $engine,
         $jscontainers->getName($jsconitem),
         $jstemplates->getName($jscontainers->getJSContainerTemplate($jsconitem)),
         $jsbranches->getName($jscontainers->getJSActiveBranch($jsconitem)),
         $owners_string
      );
    }




  }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_js_containers [ -engine|d <delphix identifier> | -all ] [ -configfile file ][-template_name template_name] [-container_name container_name]
                        [ -format csv|json ]  [ --help|? ] [ -debug ]

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
Display containers using template_name

=item B<-container_name container_name>
Display container using container_name


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

List all containers

 dx_get_js_containers -d Landshark5

 Appliance            Container name       Template name        Active branch
 -------------------- -------------------- -------------------- --------------------
 Landshark5           cont                 test                 default



=cut
