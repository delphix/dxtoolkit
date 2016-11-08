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
# Copyright (c) 2014,2016 by Delphix. All rights reserved.
#
# Program Name : dx_ctl_env.pl
# Description  : Get database and host information
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
# Modified     : 14 Mar 2015 (v2.0.0) Marcin Przepiorowski
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
use Environment_obj;
use Toolkit_helpers;
use Jobs_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name|n=s' => \(my $envname), 
  'reference|r=s' => \(my $reference),
  'action=s' => \(my $action),
  'parallel=n' => \(my $parallel),
  'debug:i' => \(my $debug), 
  'restore=s' => \(my $restore),
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


my %restore_state;

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
    @env_list = $environments->getAllEnvironments();
  };

  # for filtered databases on current engine - display status
  for my $envitem ( @env_list ) {

    my $env_name = $environments->getName($envitem);

    if ( $action eq 'enable' ) {
      if ( $environments->getStatus($envitem) eq 'enabled' ) {
        print "Environment $env_name is already enabled.\n";
      } else {
        print "Enabling environment $env_name \n";
        $jobno = $environments->enable($envitem);
      }
    }

    if ( $action eq 'disable' ) {
      if ( $environments->getStatus($envitem) eq 'disabled' ) {
        print "Environment $env_name is already disabled.\n";
      } else {
        print "Disabling environment $env_name \n";
        if ($environments->disable($envitem)) {
          print "Error while disabling environment\n";
        }
      }
    }

    if ( $action eq 'refresh' ) {
      print "Refreshing environment $env_name \n";
      $jobno = $environments->refresh($envitem);
    }

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

 dx_ctl_env [ -engine|d <delphix identifier> | -all ] 
            [ -name env_name | -reference reference ]  
            -acton <enable|disable|refresh> 
            [-help|? ] 
            [-debug ]

=head1 DESCRIPTION

Control environments

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Actions

=over 1

=item B<-action> enable|disable|refresh
Run an action specified for environments selected by filter or all environments deployed on Delphix Engine

=back

=head2 Filters

=over 2

=item B<-name>
Environment Name

=item B<-reference>
Database Name

=back

=head1 OPTIONS

=over 2

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Disabling environmanet 

 dx_ctl_env -d Landshark -name LINUXTARGET -action disable Disabling environment LINUXTARGET
 Disabling environment LINUXTARGET

Enabling environment

 dx_ctl_env -d Landshark -name LINUXTARGET -action enable 
 Enabling environment LINUXTARGET
 Starting job JOB-234 for environment LINUXTARGET.
 0 - 100
 Job JOB-234 finised with state: COMPLETED

Refreshing environment

 dx_ctl_env -d Landshark -name LINUXTARGET -action refresh
 Refreshing environment LINUXTARGET
 Starting job JOB-7544 for environment LINUXTARGET.
 0 - 40 - 100
 Job JOB-7544 finished with state: COMPLETED

=cut



