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
# Program Name : dx_ctl_network_tests.pl
# Description  : Get network test
# Author       : Marcin Przepiorowski
# Created: 11 Aug 2016 (v2.0.0)
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
use Network_obj;
use Host_obj;
use Jobs_obj;

my $version = $Toolkit_helpers::version;
my $direction = 'both';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'remoteaddr=s' => \(my $remoteaddr),
  'type=s' => \(my $type),
  'size=s' => \(my $size),
  'duration=s' => \(my $duration),
  'direction=s' => \($direction),
  'numconn=s' => \(my $numconn),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'nohead' => \(my $nohead),
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

if (! defined($remoteaddr)) {
   print "Option remoteaddr is required \n";
   pod2usage(-verbose => 1,  -input=>\*DATA);
   exit (1);
}


if (! defined($type)) {
   print "Option type is required \n";
   pod2usage(-verbose => 1,  -input=>\*DATA);
   exit (1);
}

my @directions;

if ( lc $direction eq 'both' ) {
   push (@directions, 'TRANSMIT');
   push (@directions, 'RECEIVE');
}
elsif ( lc $direction eq 'receive') {
   push (@directions, 'RECEIVE');   
}
elsif ( lc $direction eq 'transmit') {
   push (@directions, 'TRANSMIT');   
} else {
  print "Option direction has unknown value - $direction \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
} 


if ( ! ( (lc $type eq 'latency') || (lc $type eq 'throughput') || (lc $type eq 'dsp') ) ) {
  print "Option type has unknown value - $type \n";
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
    next;
   };
  
   my $net  = new Network_obj ( $engine_obj, $debug );
   my $hosts = new Host_obj    ( $engine_obj, $debug );
   
   my $testlist;

   if (defined($remoteaddr)) {
      my @templist;
      
      if (lc $remoteaddr eq 'all') {
         @templist = $hosts->getAllHosts();
      } else {
         my @hostlist = split (',', $remoteaddr);
         for my $hostitem (sort @hostlist) {
            my $hostref = $hosts->getHostByAddr($hostitem);
            if (!defined($hostref)) {
               print "Remote host with addr $hostitem not found in Delphix Engine\n";
               $ret=$ret+1;
               next;
            } else {
               push (@templist, $hostref);
            }
         }
      }

      $testlist = \@templist;

   }
   
   for my $netitem (@{$testlist}) {

      my $hostname;
      my $hostref = $net->getHost($netitem);

      if (defined($hostref)) {
         $hostname = $hosts->getHost( $hostref )->{name};
      } else {
         $hostname = 'N/A';
      }
      
      my $jobno;
      
      if (lc $type eq 'latency') {
         $jobno = $net->runLatencyTest($netitem, $size, $duration);
         if (defined ($jobno) ) {
           print "Starting job $jobno for test .\n";
           my $job = new Jobs_obj($engine_obj, $jobno, 'true', undef);
           $job->waitForJob();
         }
      } elsif (lc $type eq 'throughput') {
         for my $d (@directions) {
            $jobno = $net->runThroughputTest($netitem, $d, $numconn, $duration);
            if (defined ($jobno) ) {
              print "Starting job $jobno for test .\n";
              my $job = new Jobs_obj($engine_obj, $jobno, 'true', undef);
              $job->waitForJob();
            }
         }
      } else {
         for my $d (@directions) {
            $jobno = $net->runDSPTest($netitem, $d, $numconn, $duration);
            if (defined ($jobno) ) {
              print "Starting job $jobno for test .\n";
              my $job = new Jobs_obj($engine_obj, $jobno, 'true', undef);
              $job->waitForJob();
            }
         }      
      }
      
      

      
   }

}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_network_tests    [-engine|d <delphix identifier> | -all ] 
                         -type latency|throughput|dsp
                         [-remoteaddr env_ip|all|env_ip1,env_ip2 ] 
                         [-size bytes]  
                         [-duration sec]
                         [-direction both|transmit|receive]
                         [-numconn no_of_connections]  
                         [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the results of the network tests

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

=item B<-type latency|throughput|dsp>
select a type of test to run

=back

=head1 OPTIONS

=over 3

=item B<-remoteaddr env_ip>
Run test on:
- env_ip - environemnt IP
- all - all environments
- env_ip1,env_ip2 - comma separated list of IPs

=item B<-size bytes>
Size of latency test package

=item B<-duration sec>
Duration of the test in seconds

=item B<-direction both|transmit|receive>
Direction of dsp or throughput test

=item B<-numconn no_of_connections>
Number of connection for dsp or throughput test

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Starting latency test for 30 sec for host LINUXTARGET

 dx_ctl_network_tests -d Landshark5 -type latency -duration 30 -remoteaddr LINUXTARGET
 Starting job JOB-7643 for test .
 0 - 6 - 10 - 13 - 16 - 20 - 23 - 26 - 30 - 33 - 36 - 40 - 43 - 46 - 50 - 53 - 56 - 60 - 63 - 66 - 70 - 73 - 76 - 80 - 83 - 86 - 90 - 93 - 96 - 100
 Job JOB-7643 finished with state: COMPLETED

Starting latency test for 30 sec for host LINUXTARGET and linuxsource

 dx_ctl_network_tests -d Landshark5 -type latency -duration 30 -remoteaddr LINUXTARGET,linuxsource
 Starting job JOB-7645 for test .
 0 - 6 - 10 - 13 - 16 - 20 - 23 - 26 - 30 - 33 - 36 - 40 - 43 - 46 - 50 - 53 - 56 - 60 - 63 - 66 - 70 - 73 - 76 - 80 - 83 - 86 - 90 - 93 - 96 - 100
 Job JOB-7645 finished with state: COMPLETED
 Starting job JOB-7646 for test .
 0 - 6 - 10 - 13 - 16 - 20 - 23 - 26 - 30 - 33 - 36 - 40 - 43 - 46 - 50 - 53 - 56 - 60 - 63 - 66 - 70 - 73 - 76 - 80 - 83 - 86 - 90 - 93 - 96 - 100
 Job JOB-7646 finished with state: COMPLETED


=cut
