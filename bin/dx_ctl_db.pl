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
# Program Name : dx_ctl_db.pl
# Description  : Control VDB and dsource databases
# Author       : Marcin Przepiorowski
# Created      : 14 Mar 2015 (v2.0.0)
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
my $force = 'false';

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name=s' => \(my $dbname),
  'instance=n' => \(my $instance),
  'action=s' => \(my $action),
  'type=s' => \(my $type),
  'group=s' => \(my $group),
  'host=s' => \(my $host),
  'dsource=s' => \(my $dsource),
  'envname=s' => \(my $envname),
  'instancename=s' => \(my $instancename),
  'olderthan=s' => \(my $creationtime),
  'restore=s' => \(my $restore),
  'debug:n' => \(my $debug),
  'all' => (\my $all),
  'version' => \(my $print_version),
  'dever=s' => \(my $dever),
  'parallel=n' => \(my $parallel),
  'force=s' => \($force),
  'configfile|c=s' => \(my $config_file)
) or pod2usage(-verbose => 1,  -input=>\*DATA);


pod2usage(-verbose => 2,  -input=>\*DATA) && exit if $help;
die  "$version\n" if $print_version;
pod2usage(-verbose => 1,  -input=>\*DATA) && exit if ! ($action || $restore);

die  "$version\n" if $print_version;



my $engine_obj = new Engine ($dever, $debug);
$engine_obj->load_config($config_file);


if (defined($all) && defined($dx_host)) {
  print "Option all (-all) and engine (-d|engine) are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($action) && ( ! ( ( $action eq 'start') || ( $action eq 'stop') || ( $action eq 'enable') || ( $action eq 'disable')   ) ) ) {
  print "Option -action has invalid parameter - $action \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (defined($instance) && defined($instancename)) {
  print "Filter -instance and -instancename are mutually exclusive \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}


if (defined($instancename) && ((lc $action eq 'disable') || (lc $action eq 'enable') )) {
  print "Filter -instancename can't be specified for enable or disable action\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if ( defined($restore) ) {
  # we don't need filters
  Toolkit_helpers::check_filer_options (undef, $type, $group, $host, $dbname, $envname, $dsource);
} else {
   Toolkit_helpers::check_filer_options (1, $type, $group, $host, $dbname, $envname, $dsource, $instancename);
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

  # load objects for current engine
  my $databases = new Databases( $engine_obj, $debug);
  my $groups = new Group_obj($engine_obj, $debug);

  # filter implementation

  my $zulutime;
  if (defined($creationtime)) {
    $zulutime = Toolkit_helpers::convert_to_utc($creationtime, $engine_obj->getTimezone(), undef, 1);
  }

  my $db_list = Toolkit_helpers::get_dblist_from_filter($type, $group, $host, $dbname, $databases, $groups, $envname, $dsource, undef, $instance, $instancename, $zulutime, $debug);
  if (! defined($db_list)) {
    print "There is no DB selected to process on $engine . Please check filter definitions. \n";
    $ret = $ret + 1;
    next;
  }

  # load saved state

  my $restore_state;

  my @jobs;

  if (defined ($restore) ) {
    my $restore_file = $restore . "." . $engine;
    open (my $restore_stream, "<", $restore_file) or die ("Can't open file $restore_file for reading : $!" );
    local $/ = undef;
    my $json = JSON->new();
    $restore_state = $json->decode(<$restore_stream>);
    close $restore_stream;
  }

  my $jobno;

  # for filtered databases on current engine - display status
  for my $dbitem ( @{$db_list} ) {

    my $dbobj = $databases->getDB($dbitem);
    my $dbname = $dbobj->getName();

    if (defined ($restore) ) {

      if ( defined($restore_state->{$dbobj->getName()}) && defined($restore_state->{$dbname}->{$dbobj->getHost()}) ) {
        #set action for restore from saved state

        $action = 'donothing';

        if ( $restore_state->{$dbname}->{$dbobj->getHost()} eq 'enabled' ) {
          $action = 'enable';
        }
        if ( $restore_state->{$dbname}->{$dbobj->getHost()} eq 'disabled' ) {
          $action = 'disable';
        }

      } else {
        #skip this db
        next;
      }

    }

    if (defined($instancename)) {
      if ($dbobj->isRAC()) {
        $instance = $dbobj->getInstanceNumber($instancename);
        if (!defined($instance)) {
          print "Instance number not defined for name $instancename\n";
          $ret = $ret + 1;
          next;
        }
      } else {
        print "Target DB is not Oracle RAC. Can't use instance name to control status of instances.\n";
        $ret = $ret + 1;
        next;
      }

    }



    if ( $action eq 'start' ) {
      if ( ( $dbobj->getRuntimeStatus() eq 'RUNNING' ) && ( ! defined($instance) ) ) {
        print "Database $dbname is already started.\n";
      } elsif ( defined($instance) && ( $dbobj->getInstanceStatus($instance) eq 'up' ) ) {
        print "Instance $instance of $dbname is already started.\n";
      } else {
        if (defined($instance)) {
          print "Starting instance $instance on database $dbname.\n";
        } else {
          print "Starting database $dbname\n";
        }
        $jobno = $dbobj->start($instance);
      }
    }

    if ( $action eq 'stop' ) {
      if ( ( $dbobj->getRuntimeStatus() eq 'RUNNING' ) && ( ! defined($instance) ) ) {
        print "Stopping database $dbname.\n";
        $jobno = $dbobj->stop();
      } elsif ( defined($instance) && ( $dbobj->getInstanceStatus($instance) eq 'up' ) ) {
        print "Stopping instance $instance of database $dbname.\n";
        $jobno = $dbobj->stop($instance);
      } else {
        if (defined($instance)) {
          print "Instance $instance of database $dbname is already stopped.\n";
        } else {
          print "Database $dbname is already stopped.\n";
        }
      }
    }


    if ( $action eq 'disable' ) {
      if ( $dbobj->getEnabled() eq 'enabled' ) {
        print "Disabling database $dbname.\n";
        my $smartforce;
        if (lc $force eq 'only') {
          $smartforce = 1;
        }
        $jobno = $dbobj->disable($smartforce);
      } else {
        print "Database $dbname is already disabled.\n";
      }
    }

    if ( $action eq 'enable' ) {
      if ( $dbobj->getEnabled() eq 'disabled' ) {
        print "Enabling database $dbname.\n";
        $jobno = $dbobj->enable();
      } else {
        print "Database $dbname is already enabled.\n";
      }
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
    }

    if (defined($parallel)) {
      if ((scalar(@jobs) >= $parallel ) || (scalar(@{$db_list}) eq scalar(@jobs) )) {
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

  if ((lc $force eq 'onfailure') && (lc $action eq 'disable')) {
    # if onfailure force option for disable action was specified
    # we need to run again disable action for all objects with force flag set to yes
    # database which are already disabled will be skipped
    #$databases = new Databases( $engine_obj, $debug);
    $ret = 0;
    my $source = new Source_obj($engine_obj, $debug);
    for my $dbitem ( @{$db_list} ) {

      my $dbobj = $databases->getDB($dbitem);
      my $dbname = $dbobj->getName();
      $dbobj->refreshRuntime($source);

      if ( $dbobj->getEnabled() eq 'enabled' ) {
        print "Disabling force database $dbname.\n";
        $jobno = $dbobj->disable(1);
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
      }

      if (defined($parallel)) {
        if ((scalar(@jobs) >= $parallel ) || (scalar(@{$db_list}) eq scalar(@jobs) )) {
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


}


exit $ret;

__DATA__


=head1 SYNOPSIS

 dx_ctl_db    [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
              [ -group group_name | -name db_name | -host host_name | -type dsource|vdb | -instancename instname |  -olderthan date]
              [-instance inst_no]
              <-action start|stop|enable|disable>
              [-restore filename]
              [-force false|onfailure|only]
              [-help|? ]
              [-debug ]

=head1 DESCRIPTION

Run the action specified in action argument for all database(s) selected by filter on selected engine(s)

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

=item B<-instancename instname>
Instance name ( can be used only for Oracle RAC )


=back

=head3 Instance option

Specify an Oracle RAC instance number to perfom operation on ( this is not a filer )

=over 4

=item B<-instance inst_no>
Instance number

=back

=head2 Actions

=over 4

=item B<-action> start|stop|enable|disable
Run an action specified for all databases selected by filter

=back

=head1 OPTIONS

=over 3


=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=item B<-restore <filename> >
Restore database status using a file <filename.engine_name> generated by dx_get_db_env's save option

=item B<-parallel n>
Run action on N targets in parallel

=item B<-force false|onfailure|only>
Run action using a force option depend on argument:

=over 3

=item *
onfailure  - first attempt to disable normally; if failure, then force disable

=item *
only  - disable force only; do not attempt disable normally

=item *
false  - (default value) - only attempt disable normally

=back

=back


=head1 EXAMPLES

Stop all VDB in group Analytics

 dx_ctl_db -d Landshark -group Analytics -action stop
 Stopping database testmssql.
 Starting job JOB-229 for database testmssql.
 0 - 50 - 100
 Job JOB-229 finised with state: COMPLETED
 Stopping database testdx.
 Starting job JOB-230 for database testdx.
 0 - 40 - 50 - 100
 Job JOB-230 finised with state: COMPLETED

Start single instance (nr 1) of RAC VDB called Vracdb_FEE

 dx_ctl_db -d Delphix32 -name Vracdb_FEE -instance 1 -action start Starting instance 1 on database Vracdb_FEE.
 Starting job JOB-267177 for database Vracdb_FEE.
 0 - 25 - 45 - 100
 Job JOB-267177 finished with state: COMPLETED

Stop instances nr 1 from all RAC VDBs on single node of RAC cluster

 dx_ctl_db -d Delphix32 -host 10.0.0.58 -instance 1 -action stop
 Stopping instance 1 of database Vrac_08B.
 Starting job JOB-267182 for database Vrac_08B.
 0 - 40 - 50 - 100
 Job JOB-267182 finished with state: COMPLETED
 Stopping instance 1 of database Vracdb_FEE.
 Starting job JOB-267183 for database Vracdb_FEE.
 0 - 40 - 50 - 100
 Job JOB-267183 finished with state: COMPLETED

Disable a VDB test and force disable if soft operation will fail

 dx_ctl_db -d Landshark5 -name test -action disable -force onfailure
 Disabling database test.
 Starting job JOB-7542 for database test.
 0
 Job JOB-7542 finished with state: FAILED
 Last message is: Environment validation failed for environment "LINUXTARGET" host "LINUXTARGET". Cause: Unable to SSH to host "LINUXTARGET".
 Disabling force database test.
 Starting job JOB-7543 for database test.
 0 - 100
 Job JOB-7543 finished with state: COMPLETED

Restore state of objects saved using dx_get_db_env

 dx_ctl_db -d Landshark5 -restore /tmp/state_analytisc.bck
 Database test is already disabled.
 Enabling database test2.
 Starting job JOB-7567 for database test2.
 0 - 75 - 82 - 83 - 100
 Job JOB-7567 finished with state: COMPLETED

=cut
