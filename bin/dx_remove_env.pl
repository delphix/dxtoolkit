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
# Program Name : dx_remove_env.pl
# Description  : Remove environment
# Author       : Marcin Przepiorowski
# Created      : 07 Feb 2017 (v2.3.1)


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
use Environment_obj;
use Toolkit_helpers;
use Repository_obj;
use Jobs_obj;
use SourceConfig_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name|n=s' => \(my $envname), 
  'reference|r=s' => \(my $reference),
  'parallel=n' => \(my $parallel),
  'skip' => \(my $skip),
  'debug:i' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version)
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



my $ret = 0;
my $jobno;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    next;
  };

  # load objects for current engine
  my $environments = new Environment_obj( $engine_obj, $debug);

  # filter implementation 

  my @env_list;
  my @jobs;

  if (defined($reference)) {
    push(@env_list, $reference);
  }
  elsif (defined($envname)) {
    for my $en ( split(',', $envname) ) {
      my $env = $environments->getEnvironmentByName($en);
      if (defined($env)) {
        push(@env_list, $environments->getEnvironmentByName($en)->{reference});
      } else {
        print "Environment $en not found\n";
        $ret = $ret + 1;
        next;
      }
    }
  } else {
    print "Please provide a name(s) of environment to delete\n";
    exit 1;
  };

  # for filtered databases on current engine - display status
  for my $envitem ( @env_list ) {

    my $env_name = $environments->getName($envitem);

    print "Going to delete environment - $env_name\n";

    if (!defined ($skip)) {

      print "Are you sure (y/(n)) - use -skip to skip this confirmation \n";

      my $ok = <STDIN>;
      
      chomp $ok;

      if (($ok eq '') || (lc $ok ne 'y')) {
        print "Exiting.\n";
        exit(1);
      }

    }    

    $jobno = $environments->delete($envitem);
      
    if (defined ($jobno) ) {
      print "Starting job $jobno for environment $env_name.\n";
      my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);
      if (defined($parallel)) {
        push(@jobs, $job);
      } else {
        my $jobstat = $job->waitForJob();
        if ($jobstat ne 'COMPLETED') {
          $ret = $ret + 1;
        }
      }
    }

    if (defined($parallel)) {
      if ((scalar(@jobs) >= $parallel ) || (scalar(@env_list) eq scalar(@jobs) )) {
        my $pret = Toolkit_helpers::parallel_job(\@jobs);
        $ret = $ret + $pret;
      }
    }
    undef $jobno;  
    
  }

  if (defined($parallel) && (scalar(@jobs) > 0)) {
    while (scalar(@jobs) > 0) {
      my $pret = Toolkit_helpers::parallel_job(\@jobs);
      $ret = $ret + $pret; 
    }   
  }

}

exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_remove_env [ -engine|d <delphix identifier> | -all ] 
            -name env_name | -reference reference   
            [-skip]
            [-parallel no]
            [-help|? ] 
            [-debug ]

=head1 DESCRIPTION

Remove environment

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=over 2

=item B<-name>
Environment Name(s). Use "," to separate names.


=item B<-skip>
Skip confirmation

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Remove environment 

 dx_remove_env -d Landshark51 -name LINUXSOURCE
 Going to delete environment - LINUXSOURCE
 Are you sure (y/(n)) - use -skip to skip this confirmation
 y
 Starting job JOB-1396 for environment LINUXSOURCE.
 0 - 80 - 100
 Job JOB-1396 finished with state: COMPLETED

Remove environments in parallel mode without confirmation

 dx_remove_env -d Landshark51 -name LINUXSOURCE,LINUXTARGET -parallel 2 -skip
 Going to delete environment - LINUXSOURCE
 Starting job JOB-1400 for environment LINUXSOURCE.
 Going to delete environment - LINUXTARGET
 Starting job JOB-1401 for environment LINUXTARGET.
 Job JOB-1400 finished with status COMPLETED
 Job JOB-1401 finished with status COMPLETED


=cut



