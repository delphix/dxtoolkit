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
# Copyright (c) 2014,2019 by Delphix. All rights reserved.
#
# Program Name : dx_get_db_env.pl
# Description  : Get database and host information
# Author       : Edward de los Santos
# Created: 30 Jan 2014 (v1.0.0)
#
# Modified: 03 Mar 2015 (v1.0.2) Marcin Przepiorowski
#
# Modified: 14 Mar 2015 (v2.0.0) Marcin Przepiorowski


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
use Databases;
use Engine;
use Timeflow_obj;
use Capacity_obj;
use Formater;
use Group_obj;
use Toolkit_helpers;
use Snapshot_obj;
use Hook_obj;
use MaskingJob_obj;
use OracleVDB_obj;

my $version = $Toolkit_helpers::version;

my $parentlast = 'p';
my $hostenv = 'h';
my $configtype = 's';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name=s' => \(my $dbname),
  'format=s' => \(my $format),
  'type=s' => \(my $type),
  'rdbms=s' => \(my $rdbms),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'primary' => \(my $primary),
  'masking' => \(my $masking),
  'envname=s' => \(my $envname),
  'instance=n' => \(my $instance),
  'instancename=s' => \(my $instancename),
  'debug:i' => \(my $debug),
  'parentlast=s' =>  \($parentlast),
  'hostenv=s' =>  \($hostenv),
  'config' => \(my $config),
  'configtype=s' => \($configtype),
  'backup=s' => \(my $backup),
  'olderthan=s' => \(my $creationtime),
  'save=s' => \(my $save),
  'dever=s' => \(my $dever),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'nohead' => \(my $nohead),
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

if (defined($instance) && defined($instancename)) {
  print "Filter -instance and -instancename are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

Toolkit_helpers::check_filer_options (undef, $type, $group, $host, $dbname, $envname);

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();
my $dsource_output;

my $parentlast_head;
my $hostenv_head;

if (lc $parentlast eq 'p') {
  $parentlast_head = 'Parent snapshot';
} elsif (lc $parentlast eq 'l') {
  $parentlast_head = 'Last snapshot';
} else {
  print "Option parentlast has a wrong argument\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (lc $hostenv eq 'h') {
  $hostenv_head = 'Hostname';
} elsif (lc $hostenv eq 'e') {
  $hostenv_head = 'Env. name';
} else {
  print "Option hostenv has a wrong argument\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($rdbms)) {
  my %allowed_rdbms = (
    "oracle" => 1,
    "sybase" => 1,
    "mssql"  => 1,
    "db2"    => 1,
    "vFiles" => 1
  );

  if (!defined($allowed_rdbms{$rdbms})) {
   print "Option rdbms has a wrong argument - $rdbms\n";
   pod2usage(-verbose => 1,  -input=>\*DATA);
   exit (1);
  }

}

if (defined($backup)) {
  if (! -d $backup) {
    print "Path $backup is not a directory \n";
    exit (1);
  }
  if (! -w $backup) {
    print "Path $backup is not writtable \n";
    exit (1);
  }

  $hostenv = 'e';
  $output->addHeader(
      {'Paramters', 200}
  );

  $dsource_output = new Formater();
  $dsource_output->addHeader(
      {'Paramters', 200}
  );

  $primary = 1;

} elsif (defined($config)) {
  if ($configtype eq 's') {
    $hostenv = 'e';
    $output->addHeader(
      {'Appliance', 20},
      {'Env. name', 20},
      {'Database',   30},
      {'Group',      15},
      {'Type',        8},
      {'SourceDB',   30},
      {'Repository', 35},
      {'DB type',    10},
      {'Version',    10},
      {'Other',      100}
    );
  } elsif ($configtype eq 'd') {
    $output->addHeader(
      {'Appliance', 20},
      {$hostenv_head, 20},
      {'Database',   30},
      {'Group',      15},
      {'Type',        8},
      {'SourceDB',   30},
      {'Repository', 35},
      {'DB type',    10},
      {'Version',    15},
      {'Server DB name',  30}
    );
  } else {
    print "Configtype has to have value 'd' or 's'\n";
    exit 1;
  }
} else {
  if (defined($masking)) {
    $output->addHeader(
      {'Appliance',   20},
      {$hostenv_head, 20},
      {'Database',    30},
      {'Group',       15},
      {'Type',         8},
      {'SourceDB',    30},
      {'Masked',      10},
      {'Masking job', 15}
    );
  } else {
    $output->addHeader(
      {'Appliance'      ,20},
      {$hostenv_head    ,20},
      {'Database'       ,30},
      {'Group'          ,15},
      {'Type'            ,8},
      {'SourceDB'       ,30},
      {$parentlast_head ,35},
      {'Used(GB)'       ,10},
      {'Status'         ,10},
      {'Enabled'        ,10},
      {'Unique Name'    ,30},
      {'Parent time'    ,35},
      {'VDB creation time'    ,35}
    );
  }
}



my %save_state;

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $engine\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $capacity;
  my $timeflows;
  my $groups = new Group_obj($engine_obj, $debug);
  my $maskingjob;

  my $templates;
  my $snapshots;
  if ( defined($backup) || defined($config) ) {
      $templates = new Template_obj($engine_obj, $debug);
  } else {
    if (lc $parentlast eq 'p') {
      $snapshots = new Snapshot_obj($engine_obj, undef, undef, $debug);
    }
    $capacity = new Capacity_obj($engine_obj, $debug);
    $capacity->LoadDatabases();
    $timeflows = new Timeflow_obj($engine_obj, undef, $debug);
  }

  # filter implementation


  my $zulutime;
  if (defined($creationtime)) {
    $zulutime = Toolkit_helpers::convert_to_utc($creationtime, $engine_obj->getTimezone(), undef, 1);
  }

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, $envname, $dsource, $primary, $instance, $instancename, $zulutime, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  # filter based on rdbms type works only in dx_get_db_env

  my @db_display_list;

  if (defined($rdbms)) {
    for my $dbitem ( @{$db_list} ) {
      my $dbobj = $databases->getDB($dbitem);
      if ($dbobj->getDBType() ne $rdbms) {
        next;
      } else {
        push(@db_display_list, $dbitem);
      }
    }

    if (scalar(@db_display_list)<1) {
      print "There is no DB selected to process on $engine . Please check filter definitions. \n";
      $ret = $ret + 1;
      next;
    }

  } else {
    @db_display_list = @{$db_list};
  }


  # for filtered databases on current engine - display status
  for my $dbitem ( @db_display_list ) {
    my $dbobj = $databases->getDB($dbitem);

    my $parentsnap;
    my $snaptime;
    my $hostenv_line;
    my $timezone;
    my $parentname;
    my $parentgroup;
    my $uniquename;
    my $parenttime;


    if ($dbobj->getDBType() eq 'oracle') {
      $uniquename = $dbobj->getUniqueName();
    } else {
      $uniquename = 'N/A';
    }

    if ( $dbobj->getParentContainer() ne '' ) {
      $parentname = $databases->getDB($dbobj->getParentContainer())->getName();
      my $parentgroup_ref = $databases->getDB($dbobj->getParentContainer())->getGroup();
      $parentgroup = $groups->getName($parentgroup_ref);
    } else {
      $parentname = '';
    }

    if (lc $hostenv eq 'h') {
      $hostenv_line = $dbobj->getHost();
    } else {
      $hostenv_line = $dbobj->getEnvironmentName();
    }

    my $groupname = $groups->getName($dbobj->getGroup());

    if (defined($config)) {
      if ($configtype eq 's') {
        my $other = $dbobj->getConfig($templates, undef, $groups);
        $output->addLine(
          $engine,
          $hostenv_line,
          $dbobj->getName(),
          $groupname,
          $dbobj->getType(),
          $parentname,
          $dbobj->getHome(),
          $dbobj->{_dbtype},
          $dbobj->getVersion(),
          $other
        );
      } elsif ($configtype eq 'd') {
        my $other = $dbobj->getConfig($templates, undef, $groups);
        $output->addLine(
          $engine,
          $hostenv_line,
          $dbobj->getName(),
          $groupname,
          $dbobj->getType(),
          $parentname,
          $dbobj->getHome(),
          $dbobj->{_dbtype},
          $dbobj->getVersion(),
          $dbobj->getDatabaseName()
        );
      }

    } elsif (defined($backup)) {

      #backup($engine, $dbobj, $output, $dsource_output, $groups, $parentname, $hostenv_line, $parentgroup, $templates);
      $dbobj->getBackup($engine, $output, $dsource_output, $backup, $groupname, $parentname, $parentgroup, $templates, $groups);

    } else {

      $parentsnap = $timeflows->getParentSnapshot($dbobj->getCurrentTimeflow());

      if (lc $parentlast eq 'p') {
        if (($parentsnap ne '') && ($dbobj->getType() eq 'VDB')) {
          ($snaptime,$timezone) = $snapshots->getSnapshotTimewithzone($parentsnap);
          $parenttime = $timeflows->getParentPointTimestampWithTimezone($dbobj->getCurrentTimeflow(), $timezone);
          if (defined($parenttime) && ($parenttime eq 'N/A')) {
            my $loc = $timeflows->getParentPointLocation($dbobj->getCurrentTimeflow());
            my $lastsnaploc = $snapshots->getlatestChangePoint($parentsnap);
            if ( $loc != $lastsnaploc) {
              $parenttime = $loc;
            } else {
              $parenttime = $snaptime;
            }
          } else {
            $parenttime = 'N/A';
          }

        } else {
          $snaptime = 'N/A';
          $parenttime = 'N/A';
        }
      }

      if (lc $parentlast eq 'l') {
        my $dsource_snaps = new Snapshot_obj($engine_obj,$dbobj->getReference(), undef, $debug);
        ($snaptime,$timezone) = $dsource_snaps->getLatestSnapshotTime();
        $parenttime = 'N/A';
      }

      my $crtime;

      if (defined($dbobj->getCreationTime())) {
        $crtime = Toolkit_helpers::convert_from_utc($dbobj->getCreationTime(), $engine_obj->getTimezone())
      } else {
        $crtime = 'N/A';
      }


      if (defined($masking)) {
        my $masked;
        my $maskedjob_name;
        if ($dbobj->getMasked()) {
          $maskedjob_name = $dbobj->getMaskingJob();
          $masked = 'YES'
        } else {
          $masked = 'NO';
          $maskedjob_name = '';
        }
        $output->addLine(
          $engine,
          $hostenv_line,
          $dbobj->getName(),
          $groupname,
          $dbobj->getType(),
          $parentname,
          $masked,
          $maskedjob_name
        );
      } else {

        $output->addLine(
          $engine,
          $hostenv_line,
          $dbobj->getName(),
          $groupname,
          $dbobj->getType(),
          $parentname,
          $snaptime,
          $capacity->getDatabaseUsage($dbobj->getReference()),
          $dbobj->getRuntimeStatus(),
          $dbobj->getEnabled(),
          $uniquename,
          $parenttime,
          $crtime
        );
      }

    }

    $save_state{$dbobj->getName()}{$dbobj->getHost()} = $dbobj->getEnabled();

  }

  if ( defined($save) ) {
    # save file format - userspecified.enginename
    my $save_file = $save . "." . $engine;
    open (my $save_stream, ">", $save_file) or die ("Can't open file $save_file for writting : $!" );
    print $save_stream to_json(\%save_state, {pretty => 1});
    close $save_stream;
  }

}

if (defined($backup)) {

  my $FD;
  my $filename = File::Spec->catfile($backup,'backup_metadata_dsource.txt');

  if ( open($FD,'>', $filename) ) {
    $dsource_output->savecsv(1,$FD);
    print "Backup exported into $filename \n";
  } else {
    print "Can't create a backup file $filename \n";
    $ret = $ret + 1;
  }
  close ($FD);

  $filename = File::Spec->catfile($backup,'backup_metadata_vdb.txt');

  if ( open($FD,'>', $filename) ) {
    $output->savecsv(1,$FD);
    print "Backup exported into $filename \n";
  } else {
    print "Can't create a backup file $filename \n";
    $ret = $ret + 1;
  }
  close ($FD);

} else {
    Toolkit_helpers::print_output($output, $format, $nohead);
}


exit $ret;


__DATA__

=head1 SYNOPSIS

 dx_get_db_env    [-engine|d <delphix identifier> | -all ]
                  [-group group_name | -name db_name | -host host_name | -type dsource|vdb | -instancename instname | -olderthan date]
                  [-rdbms oracle|sybase|db2|mssql|vFiles ]
                  [-save]
                  [-masking]
                  [-parentlast l|p]
                  [-config]
                  [-backup path]
                  [-hostenv h|e]
                  [-format csv|json ]
                  [-help|? ] [ -debug ]

=head1 DESCRIPTION

Get the information about databases.

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

=head2 Filters

Filter databases using one of the following filters

=over 4

=item B<-group>
Group Name

=item B<-name>
Database Name

=item B<-host>
Host Name

=item B<-type>
Type (dsource|vdb)

=item B<-envname>
Environment name

=item B<-dsource dsourcename>
Dsource name

=item B<-instancename instname>
Instance name

=item B<-rdbms oracle|sybase|db2|mssql|vFiles>
Filter by RDBMS type - this filter is implemented only in dx_get_db_env

=back

=head3 Instance option

Specify a instance number (only with combination with host)

=over 4

=item B<-instance inst_no>
Instance number

=back

=head1 OPTIONS

=over 3

=item B<-config>
Display a config of databases (db type, version, instance / Oracle home) plus others

=item B<-masking>
Display a masking status of databases plus a masking job

=item B<-backup path>
Gnerate a dxToolkit commands to recreate databases ( Oracle / MS SQL support )
into path

=item B<-parentlast l|p>
Change a snapshot column to display :
l - a last snapshot time (default)
p - parent snapshot for VDB

=item B<-hostenv h|e>
Change a hostname/env column to display :
h - target host name (default)
e - target environment name


=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-save <filename> >
Save enabled column into JSON file <filename.engine_name> to restore it later using dx_ctl_db

=item B<-nohead>
Turn off header output

=back

=head1 COLUMNS

Columns description

=over 1

=item B<Appliance> - Delphix Engine name from dxtools.conf file

=item B<Hostname> - Delphix environment hostname or IP address

=item B<Env. name> - Delphix environment name

=item B<Database> - Database name ( dSource or VDB )

=item B<Group> - Group name

=item B<Type> - Database type: dSource | VDB | CDB (Oracle Container) | vCDB ( Oracle Virtual Container )

=item B<SourceDB> - Parent name for VDB

=item B<Parent snapshot> - Parent snapshot time for VDB

=item B<Last snapshot> - Last snapshot time for VDB or dSource

=item B<Used(GB)> - Space used by database

=item B<Status> - Runtime status of database

=item B<Enabled> - Status of database

=item B<Unique Name> - Oracle database unique name

=item B<Parent time> - Parent time used for VDB provision (it can be snapshot time or exact time selected )

=back

=head1 EXAMPLES

List all databases known to Delphix Engine

 dx_get_db_env -d Landshark51

 Appliance  Hostname             Database                       Group           Type     SourceDB                       Parent snapshot                     Used(GB)   Status     Enabled      Unique Name
 ---------- -------------------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ---------- ---------- ----------   -------------
 Landshark5 172.16.180.132       autotest                       Analytics       VDB      AdventureWorksLT2008R2         2016-12-07 10:32:26 PST             0.01       UNKNOWN    enabled      N/A
 Landshark5 172.16.180.133       AdventureWorksLT2008R2         Sources         dSource                                 N/A                                 0.00       UNKNOWN    enabled      N/A
 Landshark5 linuxsource          Oracle dsource                 Sources         dSource                                 N/A                                 0.64       RUNNING    enabled      orcl
 Landshark5 linuxsource          Sybase dsource                 Sources         dSource                                 N/A                                 0.00       RUNNING    enabled      N/A

List databases from group "Analytics" and display last snapshot time

 dx_get_db_env -d Landshark51 -group Analytics -parentlast l

 Appliance  Hostname             Database                       Group           Type     SourceDB                       Last snapshot                       Used(GB)   Status     Enabled      Unique Name
 ---------- -------------------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ---------- ---------- ----------   -------------
 Landshark5 172.16.180.132       autotest                       Analytics       VDB      AdventureWorksLT2008R2         2016-12-07 18:49:04 GMT             0.01       RUNNING    enabled      N/A

List databases created from dSource "Sybase dsource"

 dx_get_db_env -d Landshark51 -dsource "Sybase dsource"

 Appliance  Hostname             Database                       Group           Type     SourceDB                       Parent snapshot                     Used(GB)   Status     Enabled      Unique Name
 ---------- -------------------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ---------- ---------- ----------   -------------
 Landshark5 LINUXTARGET          testsybase                     Analytics       VDB      Sybase dsource                 2016-09-26 10:16:00 EDT             0.00       RUNNING    enabled      N/A
 Landshark5 LINUXTARGET          testvdb                        Tests           VDB      Sybase dsource                 2016-09-26 10:16:00 EDT             0.00       RUNNING    enabled      N/A

List all databases known to Delphix Engine showing environment name instead of hostname

 dx_get_db_env -d Landshark51 -hostenv e

 Appliance  Env. name            Database                       Group           Type     SourceDB                       Parent snapshot                     Used(GB)   Status     Enabled      Unique Name
 ---------- -------------------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ---------- ---------- ----------   -------------
 Landshark5 LINUXTARGET          autotest                       Analytics       VDB      Oracle dsource                 2016-12-08 10:44:38 EST             0.01       RUNNING    enabled      N/A
 Landshark5 LINUXTARGET          testsybase                     Analytics       VDB      Sybase dsource                 2016-09-26 10:16:00 EDT             0.00       RUNNING    enabled      N/A
 Landshark5 WINDOWSSOURCE        AdventureWorksLT2008R2         Sources         dSource                                 N/A                                 0.00       UNKNOWN    enabled      N/A
 Landshark5 LINUXSOURCE          Oracle dsource                 Sources         dSource                                 N/A                                 0.67       RUNNING    enabled      orcl
 Landshark5 LINUXSOURCE          Sybase dsource                 Sources         dSource                                 N/A                                 0.00       RUNNING    enabled      N/A
 Landshark5 LINUXTARGET          testvdb                        Tests           VDB      Sybase dsource                 2016-09-26 10:16:00 EDT             0.00       RUNNING    enabled      N/A

List databases from group "Analytics" with configuration

 dx_get_db_env -d Landshark51 -group "Analytics" -config

 Appliance  Env. name            Database                       Group           Type     SourceDB                       Repository                          DB type    Version    Other
 ---------- -------------------- ------------------------------ --------------- -------- ------------------------------ ----------------------------------- ---------- ---------- ----------------------------------------------------------------------------------------------------
 Landshark5 LINUXTARGET          autotest                       Analytics       VDB      Oracle dsource                 /u01/app/oracle/11.2.0.4/db1        oracle     11.2.0.4.0 -redoGroup 3,-redoSize 100,-archivelog=yes,-mntpoint "/mnt/provision",-instname autotest,-uniqname a
 Landshark5 LINUXTARGET          testsybase                     Analytics       VDB      Sybase dsource                 LINUXTARGET                         sybase     15.7 SP101

Generate backup of databases metadata from group "Analytics" into directory /tmp

 dx_get_db_env -d Landshark51 -group "Analytics" -backup /tmp
 Exporting database autotest hooks into  /tmp/autotest.dbhooks
 Exporting database testsybase hooks into  /tmp/testsybase.dbhooks
 Backup exported into /tmp/backup_metadata_dsource.txt
 Backup exported into /tmp/backup_metadata_vdb.txt


List masking status and jobs

 dx_get_db_env -d Delphix32 -masking

 Appliance  Hostname             Database                       Group           Type     SourceDB                       Masked     Masking job
 ---------- -------------------- ------------------------------ --------------- -------- ------------------------------ ---------- ---------------
 Delphix32  NA                   orcl_tar@LandsharkEngine       Sources@Landsha dSource                                 NO
 Delphix32  CLUSTER              racdb                          Sources         dSource                                 NO
 Delphix32  10.0.0.152           test1                          Sources         dSource                                 NO
 Delphix32  10.0.0.152           maskvdb                        Test            VDB      test1                          YES        SCOTT_JOB



=cut
