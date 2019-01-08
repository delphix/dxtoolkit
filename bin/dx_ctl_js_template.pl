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
# Copyright (c) 2016-2017 by Delphix. All rights reserved.
#
# Program Name : dx_ctl_js_template.pl.pl
# Description  : Control Delphix Engine JS template
# Author       : Marcin Przepiorowski
# Created      : 02 Mar 2017 (v2.3.2)
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
use Toolkit_helpers qw(logger trim);
use Databases;
use Group_obj;
use JS_template_obj;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'template_name=s' => \(my $template_name),
  'action=s' => \(my $action),
  'source=s@' => \(my $sources),
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



if (!defined($action) || ( ! ( (lc $action eq 'create' ) || (lc $action eq 'delete' ) ) ) ) {
  print "Action parameter not specified or has a wrong value - $action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ((lc $action eq 'create') && (!defined($sources))) {
  print "Parameter create required -source parameter\n";
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

  if (lc $action eq 'create') {
    my $databases = new Databases($engine_obj, $debug);
    my $groups = new Group_obj($engine_obj, $debug);
    my @source_array;

    for my $soitem ( @{$sources} ) {

      my @single_source = split(',', $soitem);
      if (scalar(@single_source) ne 4) {
        print "Source parameter required a 4 comma separated values - group name, database name, source name, prority\n";
        pod2usage(-verbose => 1,  -input=>\*DATA);
        exit (1);
      }

      my $source_ref = Toolkit_helpers::get_dblist_from_filter(undef, trim($single_source[0]), undef, trim($single_source[1]), $databases, $groups, undef, undef, undef, undef, undef, undef, $debug);

      if ((! defined($source_ref)) || (scalar(@{$source_ref}) < 1)) {
        print "Database " . trim($single_source[0]) . "/". trim($single_source[1]) . " not found\n";
        exit(1);
      }

      if (scalar(@{$source_ref}) > 1) {
        print "Database " . trim($single_source[0]) . "/". trim($single_source[1]) .  " is not unique\n";
        exit(1);
      }

      my @sourceline;

      $sourceline[0] = ($databases->getDB($source_ref->[0]))->getReference();
      $sourceline[1] = trim($single_source[2]);
      $sourceline[2] = trim($single_source[3]);

      push(@source_array, \@sourceline);

    }

    if ($jstemplates->createTemplate($template_name, \@source_array)) {
      print "Problem with creating a template $template_name\n";
      $ret = $ret + 1;
      next;
    } else {
      print "Template $template_name created\n";
    }
  } elsif (lc $action eq 'delete') {
    my $tempref = $jstemplates->getJSTemplateByName($template_name);
    if (!defined($tempref)) {
      print "Template name $template_name not found\n";
      $ret = $ret + 1;
      next;
    } else {
      if ($jstemplates->deleteTemplate($tempref)) {
        print "Problems with deleting template $template_name\n";
        $ret = $ret + 1;
        next;
      } else {
        print "Template $template_name deleted\n";
      }
    }


  }

}


exit $ret;



__DATA__

=head1 SYNOPSIS

 dx_ctl_js_template     [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                        -action create | delete
                        -template_name template_name
                        [ -source GroupName,DatabaseName,SourceName,Priority ]
                        [ -help|? ]
                        [ -debug ]

=head1 DESCRIPTION

Run a action on the JetStream container

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

=item B<-action create | delete>
Run an action on template

=item B<-template_name template_name>
Name of container's templates

=back

=head1 OPTIONS

=over 3

=item B<-source GroupName,DatabaseName,SourceName,Priority >
Comma separated list defining a template source (can be repeated if there are more sources)

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 Examples

Create a new templates based on one source

 dx_ctl_js_template -d Landshark51 -source "Sources, Oracle dsource, oracle, 1"  -action create -template_name template_new
 Template template_new created

Create a new templates based on two sources

 dx_ctl_js_template -d Landshark51 -source "Sources, Oracle dsource, oracle, 1" -source "Sources, Sybase dsource, sybase, 1"  -action create -template_name template2
 Template template2 created

Deleting template

 dx_ctl_js_template -d Landshark51 -action delete -template_name template2
 Template template2 deleted

=cut
