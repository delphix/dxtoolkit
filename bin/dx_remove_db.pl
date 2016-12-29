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
# Program Name : dx_remove_db.pl
# Description  : Control VDB and dsource databases
# Author       : Marcin Przepiorowski
# Created      : 12 May 2015 (v2.0.0)
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
use Databases;
use Engine;
use Jobs_obj;
use Group_obj;
use Toolkit_helpers;

my $version = $Toolkit_helpers::version;

my $timestamp = 'LATEST_SNAPSHOT';

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name=s' => \(my $dbname),  
  'type=s' => \(my $type), 
  'group=s' => \(my $group), 
  'host=s' => \(my $host),
  'skip' => \(my $skip),
  'debug:n' => \(my $debug), 
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'force' => \(my $force),
  'version' => \(my $print_version),
  'parallel=n' => \(my $parallel)
);


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


Toolkit_helpers::check_filer_options (1,$type, $group, $host, $dbname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 

my $ret = 0;

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
  
  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, undef, undef, undef, undef, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  my @jobs;

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {

    my $dbobj = $databases->getDB($dbitem);
    my $dbname = $dbobj->getName();
    my $jobno;

    my $group = $dbobj->getGroup();

    my $groupname = $groups->getName($group);

    print "Going to delete database - $groupname / $dbname\n";

    if (!defined ($skip)) {

      print "Are you sure (y/(n)) - use -skip to skip this confirmation \n";

      my $ok = <STDIN>;
      
      chomp $ok;

      if (($ok eq '') || (lc $ok ne 'y')) {
        print "Exiting.\n";
        exit(1);
      }

    }

    $jobno = $dbobj->delete($force);

    if (defined ($jobno) ) {
      print "Starting job $jobno for database $dbname.\n";
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

      if ((scalar(@jobs) >= $parallel ) || (scalar(@{$db_list}) eq scalar(@jobs) )) {
        my $pret = Toolkit_helpers::parallel_job(\@jobs);
        $ret = $ret + $pret;
      }
    }

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

 dx_remove_db [ -engine|d <delphix identifier> | -all ] 
              < -group group_name | -name db_name | -host host_name | -type dsource|vdb > 
              [ -force] 
              [ -skip] 
              [ -help|? ] 
              [ -debug ] 
              [-parallel p]

=head1 DESCRIPTION

Delete all database(s) selected by filter on selected engine(s)

=head1 ARGUMENTS

=head2 Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

=over 10

=item B<-engine|d>
Specify Delphix Engine name from dxtools.conf file

=item B<-all>
Display databases on all Delphix appliance

=back

=head2 Filters

=over 4

=item B<-group>
Group Name

=item B<-name>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)

=back 

=head1 OPTIONS

=over 3

=item B<-skip>
Skip confirmation

=item B<-force>
Force mode

=item B<-help>          
Print this screen

=item B<-debug>
Turn on debugging


=back

=head1 EXAMPLES

Remove a VDB called testsybase

 dx_remove_db -d Landshark43 -name testsybase
 Going to delete database - Analytics / testsybase
 Are you sure (y/(n)) - use -skip to skip this confirmation
 y
 Starting job JOB-824 for database testsybase.
 0 - 10 - 25 - 90 - 100
 Job JOB-824 finised with state: COMPLETED

Remove a VDB called autofs without confirmation

 dx_remove_db -d Landshark43 -name autofs -skip
 Going to delete database - Analytics / autofs
 Starting job JOB-826 for database autofs.
 0 - 25 - 100
 Job JOB-826 finised with state: COMPLETED


=cut



