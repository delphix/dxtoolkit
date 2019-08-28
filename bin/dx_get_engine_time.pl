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
# Copyright (c) 2019 by Delphix. All rights reserved.
#
# Program Name : dx_get_engine_time.pl
# Description  : Get appliance time
# Author       : Marcin Przepiorowski
# Created      : 20 Aug 2019 (v2.0.0)
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
  'tz' => \(my $tz),
  'd|engine=s' => \(my $dx_host),
  'all' => \(my $all),
  'debug:i' => \(my $debug),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);

pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;

my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

if (scalar(@{$engine_list}) > 1) {
  print "More than one engine is default. Use -d parameter\n";
  exit(3);
}

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work

  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
  }

  my $time = $engine_obj->getTime();

  if ($time eq 'N/A') {
    $ret = $ret + 1;
  }

  if (defined($tz)) {
    my $tz = $engine_obj->getTimezone();
    $time = $time . " " . $tz;
  }

  print $time . "\n";
}

exit $ret;


__DATA__

=head1 SYNOPSIS

dx_get_engine_time [ -engine|d <delphix identifier> | -all ] [ -configfile file ] [-tz]
                   [ -help|? ]

=head1 ARGUMENTS

=over 4

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script for all Delphix Engines from config file

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head1 OPTIONS

=over 4

=item B<-tz>
Print timezone with time

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display engine time

 dx_get_engine_time -d 53
 2019-08-26 17:38:14

Display engine time with timezone

 dx_get_engine_time -d 53 -tz
 2019-08-26 17:37:19 Europe/Dublin

=cut
