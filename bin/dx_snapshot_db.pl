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
# Program Name : dx_snapshot_db.pl
# Description  : Control VDB and dsource databases
# Author       : Marcin Przepiorowski
# Created      : 12 May 2015 (v2.0.0)
#

use warnings;
use strict;
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

my $usebackup = 'no';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name=s' => \(my $dbname),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'usebackup=s' => \($usebackup),
  'backupuuid=s' => \(my $backupuuid),
  'fullbackup' => \(my $fullbackup),
  'doublesync' => \(my $doublesync),
  'resync' => \(my $resync),
  'backupfileslist=s' => \(my $backupfileslist),
  'backupfilesfile=s' => \(my $backupfilesfile),
  'debug:n' => \(my $debug),
  'all' => (\my $all),
  'dever=s' => \(my $dever),
  'version' => \(my $print_version),
  'parallel=n' => \(my $parallel),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);


pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;


my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);


if (defined($usebackup) && ( ! ( (lc $usebackup eq 'yes') || (lc $usebackup eq 'no' ) ) ) ) {
  print "Option usebackup has wrong argument \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}



Toolkit_helpers::check_filer_options (1,$type, $group, $host, $dbname, undef, $dsource);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $ret = 0;

my @files_array;
if (defined($backupfileslist)) {
  @files_array = split(',',$backupfileslist);
} elsif (defined($backupfilesfile)) {
  my $FD;
  if ( ! open($FD, $backupfilesfile) ) {
    print "Can't open a file file backupset definictions\n";
    exit(1);
  }
  chomp(@files_array = <$FD>);
  close $FD;
}

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  # filter implementation

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, undef, $dsource, undef, undef, undef, undef, $debug);
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

    if ( $dbobj->getDBType() eq 'sybase') {
      $jobno = $dbobj->snapshot($usebackup, \@files_array);
    } elsif ( $dbobj->getDBType() eq 'mssql') {
      $jobno = $dbobj->snapshot($usebackup, $backupuuid);
    } elsif ( $dbobj->getDBType() eq 'oracle') {
      $jobno = $dbobj->snapshot($fullbackup, $doublesync);
    } elsif ( $dbobj->getDBType() eq 'db2') {
      $jobno = $dbobj->snapshot($resync);
    }else {
      $jobno = $dbobj->snapshot($usebackup, $resync);
    }

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
    } else {
      $ret = $ret + 1;
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

 dx_snapshot_db    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
                   < -group group_name | -name db_name | -host host_name | -type dsource|vdb >
                   [ -usebackup yes|no ]
                   [ -backupfileslist backupfile1,backupfile2,...]
                   [ -backupfilesfile /path/to/file_with_backup ]
                   [ -backupuuid uuid ]
                   [ -fullbackup ]
                   [ -doublesync ]
                   [ -help|? ]
                   [ -debug ]
                   [ -parallel p ]

=head1 DESCRIPTION

Run the snapshot for all database(s) selected by filter on selected engine(s)

=head1 ARGUMENTS

=head2 Delphix Engine selection - if not specified a default host(s) from dxtools.conf will be used.

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

=item B<-dsource name>
Name of dSource


=back

=head1 OPTIONS

=over 3

=item B<-usebackup yes|no>
For MS SQL / Sybase dSource only - if this flag is set to yes - snapshot will to loaded from last known backup, if flag is set to no Delphix Engine will run full backup
Default value is no

=item B<-backupfileslist backupfile1,backupfile2,...>
For Sybase dSource only - specify a list of backup files as a list of comma separated backup file names

=item B<-backupfilesfile /path/to/file_with_backup>
For Sybase dSource only - specify a file contains a list of backup files. One file per line

=item B<-backupuuid uuid>
For MS SQL only - UUID of backup to ingest

=item B<-fullbackup>
For Oracle only - Force full an Oracle backup

=item B<-doublesync>
For Oracle only - Enable double sync

=item B<-resync>
For plugins supporting resync

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-parallel maxjob>
Run action on all targets in parallel. Limit number of jobs to maxjob.

=back

=head1 EXAMPLES

Take snapshot of database "autoprov"

 dx_snapshot_db -d Landshark -name autoprov
 Starting job JOB-251 for database autoprov.
 0 - 95 - 100
 Job JOB-251 finised with state: COMPLETED

Take snapshot of all databases provisioned from dSource "Sybase dsource"

 dx_snapshot_db -d Landshark51 -dsource "Sybase dsource"
 Starting job JOB-191 for database testsybase.
 0 - 100
 Job JOB-191 finished with state: COMPLETED
 Starting job JOB-192 for database testvdb.
 0 - 100
 Job JOB-192 finished with state: COMPLETED

Take full backup snapshot of dSource "test121"

 dx_snapshot_db -d Landshark5 -name test121 -fullbackup
 Starting job JOB-7554 for database test121.
 0 - 3 - 8 - 15 - 17 - 22 - 29 - 33 - 39 - 45 - 50 - 54 - 58 - 62 - 65 - 70 - 75 - 78 - 100
 Job JOB-7554 finished with state: COMPLETED

=cut
