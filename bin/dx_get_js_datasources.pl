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
# Program Name : dx_get_js_datasource.pl
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
use JS_datasource_obj;
use Databases;


my $version = $Toolkit_helpers::version;


GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'template_name=s' => \(my $template_name),
  'datasource_name=s' => \(my $datasource_name),
  'dbname=s' => \(my $dbname),
  'format=s' => \(my $format), 
  'group=s' => \(my $group),
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


$output->addHeader(
    {'Appliance',         20},
    {'Datasource name',   20}, 
    {'Template name',     30},
    {'Database name',     50},
);


my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };


  my $jstemplates = new JS_template_obj ($engine_obj, $debug );
  my $db = new Databases ( $engine_obj , $debug);
  my $groups = new Group_obj($engine_obj, $debug); 
  my $db_obj;
  my $template_obj;

  if (defined($dbname)) {

    my $db_list = Toolkit_helpers::get_dblist_from_filter(undef, $group, undef, $dbname, $db, $groups, undef, undef);

    if (! defined($db_list)) {
      print "There is no DB selected to process on $engine . Please check filter definitions. \n";
      $ret = $ret + 1;
      next;
    }

    if ( ( scalar (@{$db_list} ) < 1 ) )  {
      print "Can't find database $dbname \n";
      $ret = $ret + 1;
      next;
    }

    if ( ( scalar (@{$db_list} ) > 1 ) )  {
      print "More than one database found. Please use dbname and group to select a database - $dbname \n";
      $ret = $ret + 1;
      next;
    }

    $db_obj = $db_list->[0];

  }


  if (defined($template_name)) {
    $template_obj = $jstemplates->getJSTemplateByName($template_name);
    if (! defined($template_obj)) {
      print "Template name not found\n";
      $ret = $ret + 1;
      next;
    }
  }

  my $jsdatasources = new JS_datasource_obj ( $engine_obj , $template_obj, $db_obj);

  for my $ds (@{$jsdatasources->getJSDataSourceList()}) {

      my $temp_cont = $jstemplates->getName($jsdatasources->getJSTemplate($ds));
      if (!defined($temp_cont)) {
        # do not display JS container DB's
        next;
      }

      my $display_db_name = $groups->getName($db->getDB($jsdatasources->getJSDBContainer($ds))->getGroup()). " / " . $db->getDB($jsdatasources->getJSDBContainer($ds))->getName() ;

      $output->addLine(
        $engine,
        $jsdatasources->getName($ds),
        $temp_cont,
        $display_db_name
      );

  }


}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;

__DATA__

=head1 SYNOPSIS

 dx_get_js_datasource.pl [ -engine|d <delphix identifier> | -all ] [-template_name template_name]
                         [-datasource_name datasource_name]
                         [-dbname dbname]
                         [-group group] 
                         [-format csv|json ]  
                         [-help|? ] [ -debug ]


=head1 DESCRIPTION

Get the list of Jet Stream data stores from Delphix Engine.

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

=item B<-datasource_name datasource_name>
Display data source with name datasource_name

=item B<-template_name template_name>
Display data sources for template_name

=item B<-dbname dbname>
Display data sources for database name dbname

=item B<-group groupname>
Display data sources for database name dbname and group groupname

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



