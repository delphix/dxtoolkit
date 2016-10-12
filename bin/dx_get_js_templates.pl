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
# Program Name : dx_get_js_templates.pl
# Description  : Get Delphix Engine timeflow bookmarks
# Author       : Marcin Przepiorowski
# Created      : 02 Mar 2016 (v2.2.3)
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


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'template_name=s' => \(my $template_name),
  'property_name=s' => \(my $property_name),
  'properties' => \(my $properties),
  'format=s' => \(my $format), 
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'nohead' => \(my $nohead),
  'debug:i' => \(my $debug)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 
my $output = new Formater();


if (defined($properties) || defined($property_name)) {
  $output->addHeader(
      {'Appliance',       20},
      {'Template name',   20}, 
      {'Property name',  20},
      {'Property value', 20},
  );
} else {
  $output->addHeader(
      {'Appliance',   20},
      {'Template name', 20},   
  );
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };


  my $jstemplates = new JS_template_obj ($engine_obj, $debug );


  my @template_array;


  if (defined($template_name)) {
    my $temp = $jstemplates->getJSTemplateByName($template_name);
    if (defined($temp)) {
      push(@template_array, $temp);
    } else {
      print "Template not found\n";
      $ret = $ret + 1;
      next;
    }
  } else {
    @template_array = @{$jstemplates->getJSTemplateList()};
  }



  for my $jstitem (@template_array) {

    if (defined($properties) || defined($property_name)) {

      my %prophash = %{$jstemplates->getProperties($jstitem)} ;

      if (defined($property_name)) {
        if ( defined($prophash{$property_name}) ) {
          $output->addLine(
              $engine,
              $jstemplates->getName($jstitem),
              $property_name,
              $prophash{$property_name}
            ); 
        } 

      } else {

        for my $propitem ( sort ( keys %prophash ) ) {
          $output->addLine(
              $engine,
              $jstemplates->getName($jstitem),
              $propitem,
              $prophash{$propitem}
            );                 
        }

      }

    } else {
      $output->addLine(
          $engine,
          $jstemplates->getName($jstitem)
        );  
    }

  }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_js_templates.pl [ -engine|d <delphix identifier> | -all ] [-template_name template_name] [-properties] [-property_name property_name] 
                        [ -format csv|json ]  [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Get the list of Jet Stream templates from Delphix Engine.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Options

=over 4

=item B<-template_name template_name>
Display template with a template_name

=item B<-properties>
Display properties from templates 

=item B<-property_name property_name>
Display property property_name from template 

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




=cut



