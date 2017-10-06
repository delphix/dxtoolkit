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
# Copyright (c) 2015,2016 by Delphix. All rights reserved.
# 
# Program Name : dx_logout.pl
# Description  : Logout session from cookie file
# Author       : Marcin Przepiorowski
# Created      : 14 Sep 2015 (v2.0.0)
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
use System_obj;
use Databases;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'all' => \(my $all),
  'debug:i' => \(my $debug), 
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;   

if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work

  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    exit(1);
  } 



  if ($engine_obj->logout()) {
    print "Problem with logging out from engine $engine .\n";
    $ret = 1;
  } else {
    print "Session logged out from $engine \n";
    $ret = 0;
  }
 

}

exit $ret;


__DATA__

=head1 SYNOPSIS

dx_logout  [ -d <delphix identifier> | all ] [ -help|? ]

=head1 DESCRIPTION

Logout an existing Delphix session from a cookie file

=head1 ARGUMENTS

=over 4

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

=head1 OPTIONS

=over 4

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Logout a Delphix Engine session

 dx_logout -d DE001
 Session logged out from DE001

=cut



