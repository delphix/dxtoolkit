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
# Program Name : dx_get_network_tests.pl
# Description  : Get network test
# Author       : Marcin Przepiorowski
# Created      : 11 Aug 2016 (v2.0.0)
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
use Network_obj;
use Host_obj;
use Databases;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'remoteaddr=s' => \(my $remoteaddr),
  'type=s' => \(my $type),
  'last'   => \(my $last),
  'format=s' => \(my $format),
  'debug:i' => \(my $debug),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'nohead' => \(my $nohead),
  'version' => \(my $print_version)
) or pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);

pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;

my $engine_obj = new Engine ($dever, $debug);
my $path = $FindBin::Bin;
my $config_file = $path . '/dxtools.conf';

$engine_obj->load_config($config_file);

if (defined($all) && defined($dx_host)) {
   print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
   pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
   exit (1);
}

if ((defined($last) && (! defined($remoteaddr)))) {
   print "Option -last require remoteaddr to be defined \n";
   pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
   exit (1);   
}

if (! defined($type)) {
   print "Option type is required \n";
   pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
   exit (1);
}


my $output = new Formater();



if (lc $type eq 'latency') {
  $output->addHeader(
      {'name',                 35},
      {'remote host',          15},
      {'VDB found',            10}, 
      {'state',                15},
      {'average',              10},
      {'minimum',              10},
      {'maximum',              10},
      {'stddev',               10},
      {'count',                10},
      {'size',                 10},
      {'loss',                 10}
  );
} elsif (lc $type eq 'throughput') {
  $output->addHeader(
      {'name',                 35},
      {'remote host',          15}, 
      {'VDB found',            10}, 
      {'state',                15},
      {'direction',            15},
      {'no of conn',           10},
      {'throughput',           10},
      {'block size',           10}
  );
} elsif (lc $type eq 'dsp') {
  $output->addHeader(
      {'name',                 35},
      {'remote host',          15},  
      {'state',                15},
      {'direction',            15},
      {'no of conn',           10},
      {'throughput',           10},
      {'block size',           10}
  );
} else {
  print "Option type has unknown value - $type \n";
  pod2usage(-verbose => 2, -output=>\*STDERR, -input=>\*DATA);
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
   my $databases = new Databases ( $engine_obj, $debug);
   
   my $testlist;

   if (defined($remoteaddr)) {
      my @templist;
      my @hostlist;
      if (lc $remoteaddr eq 'all') {
        @hostlist = $hosts->getAllHosts();
      } else {
        for my $h (split (',', $remoteaddr)) {
          my $hostref = $hosts->getHostByAddr($h);
          if (!defined($hostref)) {
             print "Remote host with addr $h not found in Delphix Engine\n";
             $ret=$ret+1;
             next;
          }
          push(@hostlist,$hostref);          
        }
      }
      
      
      #my @hostlist 
      for my $hostitem (sort @hostlist) {
         my $testref;
         if (defined($last)) {
            if (lc $type eq 'latency') {
              $testref = $net->getLatencyLastTests($hostitem);
              if (defined($testref)) {
                push(@templist, @{$testref});
              }
            } elsif (lc $type eq 'throughput') {
               $testref = $net->getThroughputLastTests($hostitem);
               if (defined($testref)) {
                 push(@templist, @{$testref});
               }
            } else {
               $testref = $net->getDSPLastTests($hostitem);
               if (defined($testref)) {
                 push(@templist, @{$testref});
               }
            }
         } else {
            if (lc $type eq 'latency') {
               $testref = $net->getLatencyTestsList($hostitem);
               if (defined($testref)) {
                 push(@templist, @{$testref});
               }
            } elsif (lc $type eq 'throughput') {
               $testref = $net->getThroughputLastTests($hostitem);
               if (defined($testref)) {
                 push(@templist, @{$testref});
               }
            } else {
               $testref = $net->getDSPTestsList($hostitem);
               if (defined($testref)) {
                 push(@templist, @{$testref});
               }
            }
         }
      }
      $testlist = \@templist;
      
      
      
   } else {
      if (lc $type eq 'latency') {
         $testlist = $net->getLatencyTestsList();
      } elsif (lc $type eq 'throughput') {
         $testlist = $net->getThroughputTestsList();
      } else {
         $testlist = $net->getDSPTestsList();
      }
   }
   
   for my $netitem (@{$testlist}) {
     

      my $hostname;
      my $hostref = $net->getHost($netitem);

      if (defined($hostref)) {
         $hostname = $hosts->getHost( $hostref )->{name};
      } else {
         $hostname = 'N/A';
      }
      
      my @dblist = $databases->getDBForHost($hostname);
      
      my $dbtype = grep { ($databases->getDB($_))->getType() eq 'VDB' } @dblist ; 
      my $dbtype_disp = $dbtype > 0 ? 'YES' : 'NO'; 
      
      if (lc $type eq 'latency') {
         $output->addLine(
          $net->getName($netitem),
          $hostname,
          $dbtype_disp,
          $net->getState($netitem),
          $net->getLatencyAvg($netitem),
          $net->getLatencyMin($netitem),
          $net->getLatencyMax($netitem),
          $net->getLatencyStdDev($netitem),
          $net->getLatencyCount($netitem),
          $net->getLatencySize($netitem),
          $net->getLatencyLoss($netitem)
         );
      } elsif (lc $type eq 'throughput') {
         $output->addLine(
           $net->getName($netitem),
           $hostname,
           $dbtype_disp,
           $net->getState($netitem),
           $net->getTestDirection($netitem),
           $net->getTestConnections($netitem),
           $net->getTestRate($netitem),
           $net->getTestBlockSize($netitem)
         );
      } else  {
         $output->addLine(
           $net->getName($netitem),
           $hostname,
           $net->getState($netitem),
           $net->getTestDirection($netitem),
           $net->getTestConnections($netitem),
           $net->getTestRate($netitem),
           $net->getTestBlockSize($netitem)
         );
      }
   }

}

Toolkit_helpers::print_output($output, $format, $nohead);

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_network_tests.pl [ -engine|d <delphix identifier> | -all ] -type latency|throughput
                         [ -remoteaddr env_ip ] 
                         [ -last]  
                         [ -format csv|json ]  
                         [ -help|? ] [ -debug ]

=head1 DESCRIPTION

Get the results of the network tests

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-type latency|throughput>
select a type of test to display

=back

=head1 OPTIONS

=over 3

=item B<-remoteaddr env_ip>
Filter results to a env_ip

=item B<-last>
List only last results of test for a specified env_ip


=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back




=cut
