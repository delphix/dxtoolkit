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
# Program Name : dx_get_op_template.pl
# Description  : Export hooks or hooks templates
# Author       : Marcin Przepiorowski
# Created      : 02 June 2016 (v2.1.0)

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
use Databases;
use Op_template_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name|n=s' => \(my $name), 
  'outdir=s' => \(my $outdir),
  'exportHook' => \(my $exportHook),
  'exportHookScript=s' => \(my $exportHookScript),
  'debug:i' => \(my $debug), 
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
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

if ( defined($exportHook)  && ( ! defined($outdir) ) ) {
  print "Option export require option outdir to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( defined($exportHookScript) && (!defined($name))) {
  print "Option exportHookScript require operation template name to be defined \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);  
}


# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $output = new Formater();

$output->addHeader(
    {'name',   20},
    {'type',       15},   
    {'command', 100}
);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine


  my $op_templates;
  

  $op_templates = new Op_template_obj (  $engine_obj, undef, $debug );




  my @hooks_list;

  if (defined($name)) {
    my $hook = $op_templates->getHookByName($name);
    if (!defined($hook)) {
      print "Can't find operation template - $name\n";
      $ret = $ret + 1;
    }
    push (@hooks_list, $hook);
  } else {
    @hooks_list = $op_templates->getHookList();
  }

  # for filtered databases on current engine - display status
  for my $hookitem (@hooks_list) {

    if (defined($exportHook)) {
      $op_templates->exportHookTemplate($hookitem,$outdir);
    } elsif (defined($exportHookScript)) {
      $op_templates->exportHookScript($hookitem,$exportHookScript);
    } else {
      
      $output->addLine(
        $op_templates->getName($hookitem),
        $op_templates->getType($hookitem),
        $op_templates->getCommand($hookitem)
      );
    }
  }




}

if (! ( defined($exportHook) || defined($exportHookScript) ) ) {
  Toolkit_helpers::print_output($output, $format, $nohead);
}

exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_get_op_template    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                       [ -name hook_name ] 
                       [ -outdir dir]
                       [ -exportHook ]
                       [ -exportHookScript filename]
                       [ -format csv|json ]  
                       [ -help|? ] 
                       [ -debug ] 

=head1 DESCRIPTION

List or export operation templates from engine. If no operation template name is specified all templates will be processed.

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

=head2 Filters

=over 4

=item B<-name>
Operation Template name

=item B<-group>
Group Name

=item B<-dbname>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)


=back

=head1 OPTIONS

=over 3

=item B<-exportHook>                                                                                                                                            
Export operation template into JSON file in outdir directory


=item B<-exportHookScript filename>                                                                                                                                            
Export operation template script into a specified filename

=item B<-outdir>                                                                                                                                            
Location of exported operation templates files

=item B<-format>                                                                                                                                            
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Export all operation templates from Delphix Engine

 dx_get_op_template -d Landshark5 -exportHook -outdir /tmp/a/
 Exporting operation template los into /tmp/a/after_refresh.opertemp
 Exporting operation template test1 into /tmp/a/test1.opertemp


Export operation template script 

 dx_get_op_template -d Landshark5 -name test1 -exportHookScript /tmp/test1.sh
 Exporting template into file /tmp/test1.sh



=cut



