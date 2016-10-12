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
# Program Name : dx_ctl_bundle.pl
# Description  : Get appliance information
# Author       : Marcin Przepiorowski
# Created      : 01 Sep 2016 (v2.0.0)
#


use strict;
use warnings;
use JSON;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev); #avoids conflicts with ex host and help
use File::Basename;
use Pod::Usage;
use FindBin;
use Data::Dumper;
use Date::Manip;
use File::Spec;

my $abspath = $FindBin::Bin;

use lib '../lib';
use Engine;
use System_obj;
use Toolkit_helpers;
use Jobs_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'action=s' => \(my $action),
  'dirname=s' => \(my $dirname),
  'case=s' => \(my $case), 
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

if (!defined($action)) {
  print "Argument action is required\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);  
}

if ( ! ( (lc $action eq 'download') || (lc $action eq 'upload') ) ) {
  print "Value of argument action is unknown - $action\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
} 

if ((lc $action eq 'download') && (!defined($dirname))) {
  print "Option dirname is required for action download\n";
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
    $ret = $ret + 1;
    next;
  };

  if (lc $action eq 'download') {
     
     my $system = new System_obj ($engine_obj, $debug);
     my $uuid = $system->getUUID();
     my $time = $engine_obj->getTime();
               
     my $filename = File::Spec->catfile($dirname , $uuid . '-' . UnixDate($time,'%Y%m%d-%H-%M-%S') . '.tar.gz');

     if (! -d $dirname ) {
       print "Directory $dirname doesn't exists.\n";
       $ret = $ret + 1;
       next;
     }

     if ( -w $dirname ) {
        print "Please wait. Support bundle will be genarated and saved into directory - $dirname\n";
        print "It can take several minutes\n";
     } else {
        print "Can't create file - " . basename($filename) . " - in directory $dirname \n";
        $ret = $ret + 1;
        next;
     }
 
     if ($engine_obj->generateSupportBundle($filename)) {
        print "There was a problem with support bundle generation \n";
        $ret = $ret + 1;
     } else {
        print "Support bundle for engine $engine saved into $filename \n";     
     }
  } elsif (lc $action eq 'upload') {
    my $jobno = $engine_obj->uploadSupportBundle($case);  
    if (defined ($jobno) ) {
      print "Starting job $jobno for engine $engine.\n";
      my $job = new Jobs_obj($engine_obj, $jobno, 'true', $debug);
      my $jobstat = $job->waitForJob();
      if ($jobstat ne 'COMPLETED') {
        print "There was a problem with support bundle upload \n";
        $ret = $ret + 1;
      }
    } else {
      print "There was a problem with support bundle upload \n";
      $ret = $ret + 1;
    }
    
  }
 

}

exit $ret;



__DATA__

=head1 SYNOPSIS

dx_ctl_bundle.pl  [ -d <delphix identifier> | -all ] 
                  -action download|upload  
                  [-dirname dirname ] 
                  [-case number]
                  [-debug]
                  [ -help|? ]

=head1 ARGUMENTS

Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head1 Arguments

=over 4

=item B<-action download|upload>
Action for support bundle
Download to local host or upload into Delphix Cloud 

=item B<-dirname name>
Directory where downloaded bundle will be saved

=item B<-case number>
Support case number for upload


=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Generate a support bundle and download into local file systemkey

 dx_ctl_bundle -d Landshark5 -action download -dirname ~/Documents/temp/
 Please wait. Support bundle will be genarated and saved into directory - /Users/mprzepiorowski/Documents/temp/
 It can take several minutes
 Support bundle for engine Landshark5 saved into /Users/mprzepiorowski/Documents/temp/564d1c9f-e572-7149-a48d-ad75f20107c4-20161012-12-03-45.tar.gz

Generate a support bundle and upload it into Delphix Cloud

 dx_ctl_bundle.pl -d Landshark5 -action upload -case 98765432
 Starting job JOB-7538 for engine Landshark5.
 0 - 50 - 51 - 52 - 53 - 54 - 55 - 56 - 57 - 58 - 59 - 60 - 61 - 62 - 63 - 64 - 65 - 66 - 67 - 68 - 69 - 70 - 71 - 72 - 73 - 74 - 75 - 76 - 77 - 78 - 79 - 80 - 81 - 82 - 83 - 84 - 85 - 86 - 87 - 88 - 89 - 90 - 91 - 92 - 93 - 94 - 95 - 96 - 97 - 98 - 99 - 100
 Job JOB-7538 finished with state: COMPLETED

=cut



