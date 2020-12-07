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
# Program Name : dx_ctl_dsource.pl
# Description  : Create / attach / detach dSource
# Author       : Marcin Przepiorowski
# Created      : 12 Apr 2016 (v2.2.4)
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
use FileMap;

my $version = $Toolkit_helpers::version;

my $logsync = "no";
my $compression = "no";
my $dbusertype = 'database';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'sourcename=s' => \(my $sourcename),
  'dsourcename=s'  => \(my $dsourcename),
  'action=s' => \(my $action),
  'group=s' => \(my $group),
  'creategroup' => \(my $creategroup),
  'sourceinst=s' => \(my $sourceinst),
  'sourceenv=s' => \(my $sourceenv),
  'stageinst=s' => \(my $stageinst),
  'stageenv=s' => \(my $stageenv),
  'dbuser=s'  => \(my $dbuser),
  'dbusertype=s'  => \($dbusertype),
  'password=s'  => \(my $password),
  'cdbcont=s' => \(my $cdbcont),
  'cdbuser=s' => \(my $cdbuser),
  'cdbpass=s' => \(my $cdbpass),
  'source_os_user=s'  => \(my $source_os_user),
  'stage_os_user=s'  => \(my $stage_os_user),
  'backup_dir=s' => \(my $backup_dir),
  'dumppwd=s' => \(my $dumppwd),
  'mountbase=s' => \(my $mountbase),
  'logsync=s' => \($logsync),
  'validatedsync=s' => \(my $validatedsync),
  'delphixmanaged=s' => \(my $delphixmanaged),
  'hadr=s' => \(my $hadr),
  'compression=s' => \($compression),
  'type=s' => \(my $type),
  'dever=s' => \(my $dever),
  'debug:n' => \(my $debug),
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


if ( (! defined($action) ) || ( ! ( ( $action eq 'create') || ( $action eq 'attach') || ( $action eq 'detach') || ( $action eq 'update') ) ) ) {
  print "Option -action not defined or has invalid parameter - $action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if (! (($action eq 'detach') || ($action eq 'update')) )  {

  if (defined($cdbcont) && ((!defined($cdbpass)) || (!defined($cdbuser)))) {
    print "Option -cdbcont required a cdbpass and cdbuser to be defined \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (!defined($type)) {
    print "Option -type is required for this action \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if ( defined ($type) && ( ! ( ( lc $type eq 'oracle') || ( lc $type eq 'sybase') || ( lc $type eq 'mssql') || ( lc $type eq 'vfiles') || ( lc $type eq 'db2') ) ) ) {
    print "Option -type has invalid parameter - $type \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (((lc $type eq 'vfiles') || (lc $type eq 'db2')) && (lc $action eq 'attach')) {
    print "Can't attach $type dSource\n";
    exit (1);
  }

  if ( ( lc $type ne 'db2' ) && ( ! ( defined($type) && defined($sourcename) && defined($dsourcename)  && defined($source_os_user) && defined($group) ) ) )  {
    print "Options -sourcename, -dsourcename, -group, -source_os_user are required. \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }


  if (( lc $type ne 'db2' ) && ( lc $type ne 'vfiles' ) && (! ( defined($dbuser) && defined($password)  ) ) ) {
    print "Options -dbuser and -password are required for non vFiles dsources. \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if (( lc $type eq 'sybase' ) && ( ! ( defined($stage_os_user) && defined($stageinst) && defined($stageenv) && defined($backup_dir) && defined($sourceinst) && defined($sourceenv) ) ) ) {
    print "Options -stage_os_user, -stageinst, -stageenv, -sourceinst, -sourceenv and -backup_dir are required. \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }

  if ( defined($logsync) && ( ! ( ( lc $logsync eq 'yes') || ( lc $logsync eq 'no')  ) ) ) {
    print "Options -logsync has yes and no value only. \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
  }


} else {
  if (defined ($type) && ((lc $type eq 'vfiles') || (lc $type eq 'db2') ) && (lc $action eq 'detach')) {
    print "Can't deattach $type dSource\n";
    exit (1);
  }

  if (( ! ( defined($group) ) ) && ( ! ( defined($dsourcename)  ) ) ) {
    print "Options  -dsourcename or -group are required to detach or update. \n";
    pod2usage(-verbose => 1,  -input=>\*DATA);
    exit (1);
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

  my $db;
  my $jobno;

  my $groups = new Group_obj($engine_obj, $debug);

  if ((lc $action eq 'detach') || (lc $action eq 'update')) {
    my $databases = new Databases($engine_obj,$debug);

    my $source_ref = Toolkit_helpers::get_dblist_from_filter(undef, $group, undef, $dsourcename, $databases, $groups, undef, undef, undef, undef, undef, undef, $debug);

    if (!defined($source_ref)) {
      print "Source database not found.\n";
      $ret = $ret + 1;
      next;
    }

    for my $dbref (@{$source_ref}) {

      my $source = ($databases->getDB($dbref));

      # only for sybase and mssql
      my $type = $source->getDBType();
      if ($action eq 'detach')  {
        $jobno = $source->detach_dsource();
        $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with dSource action");
      } elsif (($type eq 'sybase') || ($type eq 'mssql')) {
        $jobno = $source->update_dsource($backup_dir, $logsync, $validatedsync);
        if (defined($jobno)) {
          $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with dSource action");
        }
        $jobno = $source->update_dsource_config( $stageenv, $stageinst );
        if (defined($jobno)) {
          $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with dSource config action");
        }
      }

    }

  } elsif ($action eq 'attach')  {
    my $databases = new Databases($engine_obj,$debug);

    my $source_ref = Toolkit_helpers::get_dblist_from_filter(undef, $group, undef, $dsourcename, $databases, $groups, undef, undef, undef, undef, undef, undef, $debug);

    if (!defined($source_ref)) {
      print "Source database not found.\n";
      $ret = $ret + 1;
      next;
    }
    elsif (scalar(@{$source_ref})>1) {
      print "Source database not unique defined.\n";
      $ret = $ret + 1;
      next;
    } elsif (scalar(@{$source_ref}) eq 0) {
      print "Source database not found.\n";
      $ret = $ret + 1;
      next;
    }

    # there will be only one database object in the list so we need to assign it to obj variable
    my $source = ($databases->getDB($source_ref->[0]));

    if ( $type eq 'oracle' ) {
      $jobno = $source->attach_dsource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$stageenv,$stageinst,$stage_os_user, $backup_dir);
    } else {
      $jobno = $source->attach_dsource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$stageenv,$stageinst,$stage_os_user, $backup_dir, $validatedsync, $delphixmanaged, $compression, $dbusertype);
    }


    # you can attach only one dSource at the time so one job
    $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with dSource action");

  } elsif ($action eq 'create') {

    # create a group for new dSource
    if (! defined($groups->getGroupByName($group))) {
      if (defined($creategroup)) {
        print "Creating not existing group - $group \n";
        my $jobno = $groups->createGroup($group);
        my $actionret = Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with group creation");
        if ($actionret > 0) {
          $ret = $ret + 1;
          print "There was a problem with group creation. Skipping source actions on engine\n";
          next;
        }
      } else {
        print "Group $group for target database doesn't exist.\n Skipping source actions on engine.\n";
        $ret = $ret + 1;
        next;
      }
    }


    if ( $type eq 'oracle' ) {
      my $db = new OracleVDB_obj($engine_obj,$debug);

      if (defined($cdbcont)) {
        if ($db->discoverPDB($sourceinst,$sourceenv,$cdbcont,$cdbuser,$cdbpass)) {
          print "There was an error with PDB discovery \n";
          $ret = $ret + 1;
          next;
        }
      }
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync);
    }
    elsif ($type eq 'sybase') {
      my $db = new SybaseVDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync,$stageenv,$stageinst,$stage_os_user, $backup_dir, $dumppwd, $mountbase);
    }
    elsif ($type eq 'mssql') {
      my $db = new MSSQLVDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync,$stageenv,$stageinst,$stage_os_user, $backup_dir, $dumppwd, $validatedsync, $delphixmanaged, $compression, $dbusertype);
    }
    elsif ($type eq 'vFiles') {
      my $db = new AppDataVDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dsourcename,$group);
    }
    elsif ($type eq 'db2') {
      my $db = new DB2VDB_obj($engine_obj,$debug);
      $jobno = $db->addSource($sourcename,$sourceinst,$sourceenv,$source_os_user,$dbuser,$password,$dsourcename,$group,$logsync,$stageenv,$stageinst,$stage_os_user, $backup_dir, $hadr);
    }

    # we are adding only one dSource - so one job
    $ret = $ret + Toolkit_helpers::waitForAction($engine_obj, $jobno, "Action completed with success", "There were problems with dSource action");

  }



}

exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_ctl_dsource [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
  -action create, attach, detach
  -type dsourcetype
  -sourcename name
  -dsourcename dsourcename
  -group groupname
  -sourceinst source_instance
  -sourceenv source_environment
  -dbuser username
  -password password
  -source_os_user osusername
  [-creategroup]
  [-logsync yes/no ]
  [-stageinst staging_inst ]
  [-stageenv staging_env ]
  [-stage_os_user staging_osuser ]
  [-backup_dir backup_dir ]
  [-dumppwd password ]
  [-mountbase mountpoint ]
  [-validatedsync mode ]
  [-delphixmanaged yes/no ]
  [-dbusertype database|environment|domain]
  [-cdbcont container -cdbuser user -cdbpass password]
  [-debug ]
  [-version ]
  [-help|? ]

=head1 DESCRIPTION

Create or attache dSource to a Delphix Engine

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

=back

=head2 dSource arguments

=over 1

=item B<-type>
Type (oracle|sybase|mssql|db2|vfiles)

=item B<-action>
Action - create, attach, detach, update

Update action can change a backup path and validated sync mode for MS SQL and Sybase dsources

=item B<-group>
Source Group Name

=item B<-sourcename>
Database Name

=item B<-dsourcename>
dSource name

=item B<-sourceinst source_instance>
Source database instance / home

=item B<-sourceenv source_environment>
Source database environment name

=item B<-dbuser username>
Database user

=item B<-password password>
Database password

=item B<-source_os_user osusername>
Source database os user

=item B<-stageinst staging_inst>
Staging database instance

=item B<-stageenv staging_env>
Staging database environment


=item B<-stage_os_user staging_osuser>
Staging database os user

=item B<-backup_dir backup_dir>
Backup location. From Delphix 5.2.3 multiple backup locations with comma separation can be specified
for MS SQL dSource.

=item B<-logsync yes/no>
Enable or no LogSync for dSource. Default LogSync is disabled.

=item B<-dumppwd password>
Password for backup used to create dsource

=item B<-mountbase mountpoint>
For Sybase only - mount point for staging server

=item B<-validatedsync mode>
Set validated sync mode.

Allowed values for MS SQL:
TRANSACTION_LOG, FULL, FULL_OR_DIFFERENTIAL, NONE

Allowed values for Sybase:
DISABLED, ENABLED

=item B<-delphixmanaged yes/no>
Use Delphix Manage backup mode for MS SQL

=item B<-cdbcont container>
Oracle only - CDB container for a PDB dSource

=item B<-cdbuser user>
Oracle only - CDB user for a PDB dSource

=item B<-cdbpass password>
Oracle only - CDB password for a PDB dSource

=item B<-creategroup>
Create a Delphix group if it doesn't exist

=item B<-dbusertype database|environment|domain>
Specify a database user type for MS SQL. Default value is database.

=item B<-hadr hadrPrimarySVC:XXX,hadrPrimaryHostname:hostname,hadrStandbySVC:YYY>
Add DB2 dSource with HADR support
Parameter hadrTargetList is optional.

ex.
hadrPrimarySVC:50001,hadrPrimaryHostname:marcindb2src.dcenter,hadrStandbySVC:50011,hadrTargetList:marcindb2src.dcenter:50001


=back

=head1 OPTIONS

=over 2

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Create a Sybase dSource from database called pub3 running on instance LINUXSOURCE discovered in environment LINUXSOURCE,
staging environment is on LINUXTARGET environment with instance named LINUXTARGET

 dx_ctl_dsource -d Landshark5 -type sybase -sourcename pubs3 -sourceinst LINUXSOURCE -sourceenv LINUXSOURCE \
                -source_os_user delphix -dbuser sa -password delphixdb -group Sources -dsourcename "Sybase dsource" \
                -stage_os_user delphix -stageinst LINUXTARGET -stageenv LINUXTARGET -backup_dir "/u02/sybase_back"
                -action create -dumppwd xxxxxx
 Waiting for all actions to complete. Parent action is ACTION-2995
 Action completed with success.

Create an Oracle dSource from database unique name TESTU running from
Oracle Home "/u01/app/oracle/product/11.2.0/dbhome_1" discovered in environment LINUXSOURCE

 dx_ctl_dsource -d Landshark5 -type oracle -sourcename TESTU -sourceinst /u01/app/oracle/product/11.2.0/dbhome_1 \
                -sourceenv LINUXSOURCE -source_os_user delphix -dbuser delphixdb -password delphixdb -group Sources \
                -dsourcename "ORACLE dsource" -action create
 Waiting for all actions to complete. Parent action is ACTION-3011
 Action completed with success.

Create an MSSQL dSource from database unique name AdventureWorksLT2008R2 running on MSSQLSERVER instance discovered in
environment WINDOWSSOURCE, staging environment is on WINDOWSTARGET environment with instance named MSSQLSERVER

 dx_ctl_dsource -d Landshark5 -type mssql -sourcename AdventureWorksLT2008R2 -sourceinst MSSQLSERVER \
                -sourceenv WINDOWSSOURCE -source_os_user "DELPHIX\delphix_admin" -dbuser aw -password delphixdb \
                -group Sources -dsourcename AdventureWorksLT2008R2 -stage_os_user "DELPHIX\delphix_admin"
                -stageinst MSSQLSERVER - stageenv WINDOWSTARGET -backup_dir "\\\\172.16.180.133\\backups" -action create
 Waiting for all actions to complete. Parent action is ACTION-3050
 Action completed with success.

Detach dsource

 dx_ctl_dsource -d Landshark5 -action detach -dsourcename "Sybase dsource"
 Waiting for all actions to complete. Parent action is ACTION-3050
 Action completed with success.

Attach Sybase dsource

 dx_ctl_dsource -d Landshark5 -action attach -type sybase -sourcename pubs3 -sourceinst LINUXSOURCE -sourceenv LINUXSOURCE \
                -source_os_user delphix -dbuser sa -password delphixdb -group Sources -dsourcename "Sybase dsource" \
                -stage_os_user delphix -stageinst LINUXTARGET -stageenv LINUXTARGET -backup_dir "/u02/sybase_back"
 Waiting for all actions to complete. Parent action is ACTION-12699
 Action completed with success

Attach Oracle dsource

 dx_ctl_dsource -d Landshark5 -action attach -type oracle -sourcename TESTU -sourceinst /u01/app/oracle/product/11.2.0/dbhome_1 \
                              -sourceenv LINUXSOURCE -source_os_user delphix -dbuser delphixdb -password delphixdb \
                              -group Sources -dsourcename "Oracle dsource"
 Waiting for all actions to complete. Parent action is ACTION-12691
 Action completed with success

Adding an Oracle PDB dSource

 dx_ctl_dsource -d Landshark5 -action create -sourcename PDB1 -type oracle -sourceinst /u01/app/oracle/12.2.0.1/db1 \
                              -sourceenv LINUXSOURCE -source_os_user oracle -dbuser delphixdb -password delphixdb -group Sources \
                              -dsourcename PDB1 -cdbcont test122 -cdbuser c##delphixdb -cdbpass delphixdb
 Setting credential for CDB test122 sucessful.
 Waiting for all actions to complete. Parent action is ACTION-13947
 Action completed with success

Adding a DB2 dSource without HADR
 dx_ctl_dsource -d 531 -stage_os_user auto1052 -stageenv marcindb2tgt -stageinst "auto1052 - 10.5.0.5 - db2aese" -action create -type db2  \
                       -sourcename R74D105D -dsourcename dsourceR74D105D -group Untitled -backup_dir "/db2backup"
 Waiting for all actions to complete. Parent action is ACTION-1870
 Action completed with success

Adding a DB2 dSource with HADR

 dx_ctl_dsource -d 531 -stage_os_user auto1052 -stageenv marcindb2tgt -stageinst "auto1052 - 10.5.0.5 - db2aese" -action create -type db2 \
                       -sourcename R74D105E  -dsourcename R74D105E -group Untitled -backup_dir "/db2backup" \
                       -hadr "hadrPrimarySVC:50001,hadrPrimaryHostname:marcindb2src.dcenter,hadrStandbySVC:50011,hadrTargetList:marcindb2src.dcenter:50001"
 Waiting for all actions to complete. Parent action is ACTION-1879
 Action completed with success

Updating a backup path and validated sync mode for Sybase

 dx_ctl_dsource -d Landshark5 -action update -validatedsync ENABLED -backup_dir "/u02/sybase_back" -dsourcename pubs3
 Waiting for all actions to complete. Parent action is ACTION-20194
 Action completed with success

 Updating a backup path and validated sync mode for MS SQL

  dx_ctl_dsource -d Landshark5 -action update -validatedsync FULL -backup_dir "\\\\172.16.180.10\\loc1,\\\\172.16.180.10\\loc2" -dsourcename AdventureWorks2012
  Waiting for all actions to complete. Parent action is ACTION-20190
  Action completed with success


Update a staging server and instace for Sybase or MS SQL

  dx_ctl_dsource -d Landshark5 -action update -dsourcename pubs3 -backup_dir /u02/sybase_backup -stageinst LINUXTARGET -stageenv linuxtarget
  Waiting for all actions to complete. Parent action is ACTION-8576
  Action completed with success
  Waiting for all actions to complete. Parent action is ACTION-8577
  Action completed with success


Update a staging server and instace for Sybase or MS SQL based on group

  dx_ctl_dsource -d Landshark5 -action update -group SybaseSource -backup_dir /u02/sybase_backup -stageinst LINUXTARGET -stageenv linuxtarget
  Waiting for all actions to complete. Parent action is ACTION-8593
  Action completed with success
  Waiting for all actions to complete. Parent action is ACTION-8594
  Action completed with success


=cut
