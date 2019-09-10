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

# Program Name : dx_ctl_engine_upgrade.pl
# Description  : Upload upgrade into Delphix Engine and allow apply / verify actions
# Author       : Marcin Przepiorowski
# Created      : April 2019
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use File::Spec;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use Formater;
use Toolkit_helpers;
use Storage_obj;
use Jobs;


my $version = $Toolkit_helpers::version;
my $gradeonly = 'yes';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'action=s' => \(my $action),
  'osname=s' => \(my $osname),
  'filename=s' => \(my $filename),
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

if (!defined($action)) {
  print "Parameter -action is required \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (!((lc $action eq 'upload') || (lc $action eq 'verify') || (lc $action eq 'apply'))) {
  print "Parameter -action has to be one of the following: upload, verify or apply \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (((lc $action eq 'verify') || (lc $action eq 'apply')) && (!defined($osname))) {
  print "Parameter -osname is required for verify or upload action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


my $file_version;

if (lc $action eq 'upload')  {
  if (!defined($filename)) {
    print "Parameter -filename is required for upload action \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  } else {

    my $namechek = basename($filename);
    if ( ! (($file_version) = $namechek =~ /^delphix_(\d.\d.\d.\d)_\d\d\d\d-\d\d-\d\d-\d\d-\d\d.upgrade.tar.gz$/ )) {
      print "Filename is not matching delphix upgrade pattern \n";
      exit (1);
    }

    if (!defined($file_version)) {
      print "Filename is not matching delphix upgrade pattern. Can't find version number \n";
      exit (1);
    }

  }
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
  print "Can't connect to Dephix Engine $dx_host\n\n";
  $ret = $ret + 1;
  next;
  };

  if ($engine_obj->getCurrentUserType() ne 'SYSTEM') {
   print "User with sysadmin role is required for this script to run. Please check dxtools.conf entry for $engine\n";
   next;
  }




  my $jobstart;

  if (lc $action eq 'upload') {

    my $osver = $engine_obj->getOSversions();
    if (defined($osver->{$file_version})) {
      print "Version detected in upgrade file $file_version is already uploaded or running on Delphix Engine\n";
      $ret = $ret + 1;
      next;
    }

    $jobstart = Toolkit_helpers::timestamp("-0min", $engine_obj);

    my $rc = $engine_obj->uploadupdate($filename);

    if ($rc ne 0) {
      $ret = $ret + $rc;
      next;
    }

    print "Checking status of upload verification job\n";


    my $jobs = new Jobs($engine_obj, $jobstart, undef, undef, undef, undef, undef, undef, 1, undef, $debug);
    my $counter = 0;
    my @refresh;
    my $joblist;

    do {
      $jobs->loadJobs();
      $joblist = $jobs->getJobList();
      if (version->parse($engine_obj->getApi()) >= version->parse(1.10.0)) {
        @refresh = grep { ($jobs->getJob($_)->getJobActionType()) eq 'UNPACK_VERSION' } @{$joblist};
      } else {
        @refresh = grep { ($jobs->getJob($_)->getJobActionType()) eq 'REFRESH_VERSIONS' } @{$joblist};
      }
      sleep 10;
      $counter = $counter + 1;
      if ($counter > 18) {
        print "There is no job started for 3 minutes - exiting. File is uploaded please check GUI for job\n";
        $ret = $ret + 1;
        next;
      }
    }
    while (scalar(@refresh)<1);

    my $job = $jobs->getJob($refresh[-1]);
    my $retjob = $job->waitForJob();
    if ($retjob eq 'COMPLETED') {
      print "Uncompressing job " . $refresh[-1] . " finished.\n";
    } else {
      $ret = $ret + 1;
      next;
    }


    if (version->parse($engine_obj->getApi()) >= version->parse(1.9.0)) {
      # above 5.2 there is a verification job
      $counter = 0;
      $jobs = new Jobs($engine_obj, $jobstart, undef, undef, undef, undef, undef, undef, 1, undef, $debug);
      do {
        $jobs->loadJobs();
        $joblist = $jobs->getJobList();
        @refresh = grep { ($jobs->getJob($_)->getJobActionType()) eq 'UPGRADE_VERIFY' } @{$joblist};
        sleep 10;
        $counter = $counter + 1;
        if ($counter > 18) {
          print "There is no verification job started for 3 minutes - exiting. File is uploaded please check GUI for job\n";
          $ret = $ret + 1;
          next;
        }
      } while (scalar(@refresh)<1);

      $job = $jobs->getJob($refresh[-1]);
      $retjob = $job->waitForJob();
      if ($retjob eq 'COMPLETED') {
        print "Verification job $job finished\n";
      } else {
        $ret = $ret + 1;
      }


    }
  }

  if (lc $action eq 'verify') {

    my $jobno = $engine_obj->verifyOSversion($osname);

    if (defined($jobno)) {
      $ret = $ret + Toolkit_helpers::waitForJob($engine_obj, $jobno, "Verification OK", "Verification job failed");
    } else {
      $ret = $ret + 1;
    }

  }

  if (lc $action eq 'apply') {

    my $jobno = $engine_obj->applyOSversion($osname);

    if (defined($jobno)) {
      $ret = $ret + Toolkit_helpers::waitForJob($engine_obj, $jobno, "Apply job finished. Restarting.", "Apply job failed");
    } else {
      $ret = $ret + 1;
    }

  }

}



exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_ctl_engine_upgrade    [-engine|d <delphix identifier> | -all ]
                          -action upload|verify|apply
                          [-osname osname]
                          [-filename filename]
                          [-help|? ] [ -debug ]

=head1 DESCRIPTION

Script allows to upload, verify or apply a Delphix Engine upgrade.

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=item B<-action upload|verify|apply>
Run a particular action

=item B<-osname nameofversion>
Select a version of OS for action - requried for verify or apply

=item B<-filename upgradefilename>
Select an upgrade file for upload

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head1 OPTIONS

=over 3


=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLE



=cut
