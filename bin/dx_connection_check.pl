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
# Program Name : dx_connection_check.pl
# Description  : Check if port is open
# Author       : Marcin Przepiorowski
# Created      : Dec 2019
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
use Host_obj;
use Action_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'hostip=s' => \(my $hostip),
  'port=i' => \(my $port),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
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


if (! (defined($hostip) && defined($port) ) ) {
  print "Option hostip and port are required. \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);


my %restore_state;

my $ret = 0;


for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  my ($connfault, $reason) = $engine_obj->checkSSHconnectivity("dummy", "password", $hostip, $port);

  if (version->parse($engine_obj->getApi()) >= version->parse(1.10.0)) {
    if ($connfault == 0) {
      print "Connection using SSH with dummy user and password is sucessful\n";
      next;
    }
    my $out = $reason->{"error"}->{"details"};
    if ($out =~ m/An error occurred when attempting/) {
      print "Connection to $hostip:$port refused - port closed\n";
      $ret = $ret + 1;
    } elsif ($out =~ m/Could not log into/) {
      print "Connection to $hostip:$port sucessful - port seems to be opened\n";
    } else {
      print "Engine responded with unknown state. Please check below:\n";
      print Dumper $reason->{"details"};
      print Dumper $reason->{"commandOutput"};
      $ret = $ret + 1;
    }

  }

}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_connection_check  [ -engine|d <delphix identifier> | -all ] [ -configfile file ] -hostip IP/FQDN -port portno [ --help|? ] [ -debug ]

=head1 DESCRIPTION

Check connectivity between Delphix Engine and host using a specified port

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Run script on all engines

=item B<-hostip IP/FQDN>
Host IP or FQDN

=item B<-port portno>
Port to check


=back

=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Check if listener port is open

 dx_connection_check -d 53 -hostip 192.168.1.20 -port 1521
 Connection to 192.168.1.20:1521 refused - port closed

Check if ssh port is open

 dx_connection_check -d 53 -hostip 192.168.1.20 -port 22
 Connection to 192.168.1.20:22 sucessful - port seems to be opened

=cut
