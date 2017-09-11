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
# Copyright (c) 2017 by Delphix. All rights reserved.
# 
# Program Name : dx_get_autostart.pl
# Description  : Get autostart status for VDB
# Author       : Marcin Przepiorowski
# Created      : 24 Jan 2017 (v2.3.1)
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
use Formater;
use Toolkit_helpers;
use Databases;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'dbname=s' => \(my $dbname), 
  'group=s'  => \(my $group),
  'host=s' => \(my $host),
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
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


Toolkit_helpers::check_filer_options (undef,undef, $group, $host, $dbname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 


my %restore_state;

my $ret = 0;

my $output = new Formater();

$output->addHeader(
  {'Appliance',         20},
  {'Database name',     30},
  {'Group name',        30},
  {'Autostart',         10}
);


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);  

  # filter implementation 

  my $db_list = Toolkit_helpers::get_dblist_from_filter('VDB', $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = 1;
    next;
  }


  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {
    my $dbobj = $databases->getDB($dbitem);

    $output->addLine(
      $engine,
      $dbobj->getName(),
      $groups->getName($dbobj->getGroup()),
      $dbobj->getAutostart()
    );

  }


}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_autostart [ -engine|d <delphix identifier> | -all ] 
                  [ -group group_name | -dbname db_name | -host host_name ]  
                  [ -help|? ] 
                  [ -debug ]

=head1 DESCRIPTION

Get status of VDB auto start 

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script on all engines

=item B<-group>
Group Name

=item B<-dbname>
Database Name

=item B<-host>
Host Name

=back

=head1 OPTIONS

=over 2

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Displaying status for all VDB's

 dx_get_autostart -d Landshark51
 
 Appliance            Database name                  Group name                     Autostart
 -------------------- ------------------------------ ------------------------------ ----------
 Landshark51          test1                          Analytics                      yes
 Landshark51          testdx                         Analytics                      yes
 Landshark51          testsys                        Analytics                      no

Displaying status for VDB called testsys

 dx_get_autostart -d Landshark51 -dbname testsys

 Appliance            Database name                  Group name                     Autostart
 -------------------- ------------------------------ ------------------------------ ----------
 Landshark51          testsys                        Analytics                      no


=cut



