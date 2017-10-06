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
use Repository_obj;
use Jobs_obj;
use SourceConfig_obj;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help), 
  'd|engine=s' => \(my $dx_host), 
  'name|n=s' => \(my $envname), 
  'reference|r=s' => \(my $reference),
  'action=s' => \(my $action),
  'username=s' => \(my $username),
  'authtype=s' => \(my $authtype),
  'password=s' => \(my $password),
  'repotype=s' => \(my $repotype),
  'repopath=s' => \(my $repopath),
  'vfilepath=s' => \(my $vfilepath),
  'dbname=s'   => \(my $dbname),
  'uniquename=s' => \(my $uniquename),
  'instancename=s' => \(my $instancename),
  'jdbc=s'     => \(my $jdbc),
  'listenername=s' => \(my $listenername), 
  'endpoint=s@' => \(my $endpoint),
  'bits=n' => \(my $bits),
  'ohversion=s' => \(my $ohversion), 
  'oraclebase=s' => \(my $oraclebase),
  'parallel=n' => \(my $parallel),
  'debug:i' => \(my $debug), 
  'restore=s' => \(my $restore),
  'dever=s' => \(my $dever),
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

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj); 


if (!defined($action)) {
  print "Action has to be defined\n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

if (!((lc $action eq 'refresh') || (lc $action eq 'enable')  || (lc $action eq 'disable') ||
    (lc $action eq 'addrepo') || (lc $action eq 'deleterepo') || 
    (lc $action eq 'adddatabase') || (lc $action eq 'deletedatabase') || 
    (lc $action eq 'addlistener') || (lc $action eq 'deletelistener') ||
    (lc $action eq 'adduser') || (lc $action eq 'deleteuser')
    )) 
    {
      print "Unknown action $action\n";
      pod2usage(-verbose => 1,  -input=>\*DATA);
      exit (1);
} 

if (lc $action eq 'addrepo') {
  if (!defined($repotype)) {
    print "Repository type has to be set\n";
    exit 1;
  }
}

if ((lc $action eq 'addrepo') || (lc $action eq 'deleterepo')) {
  if (!defined($repopath)) {
    print "Repository path or instance has to be set\n";
    exit 1;
  }
}

if (lc $action eq 'adddatabase') {
  if (defined($repotype)) {
    if (lc $repotype eq 'oracle') {
      if (!defined($repopath)) {
        print "Repository path or instance has to be set\n";
        exit 1;
      }
      if (!defined($uniquename)) {
        print "uniquename has to be set\n";
        exit 1;
      }
      if (!defined($instancename)) {
        print "instancename has to be set\n";
        exit 1;
      }
      if (!defined($jdbc)) {
        print "jdbc has to be set\n";
        exit 1;
      }
    } elsif (lc $repotype eq 'vfiles') {
      if (!defined($vfilepath)) {
        print "vfilepath has to be set\n";
        exit 1;
      }
    } else {
      print "Repotype parameter $repotype unknown. Use oracle or vfiles\n";
      exit 1;
    }
  } else {
    print "Repotype parameter is required with adddatabase\n";
    exit 1;
  }
}

if ((lc $action eq 'adddatabase') || (lc $action eq 'deletedatabase')) {
  if (!defined($repotype)) {
    print "Repotype parameter is required with deletedatabase\n";
    exit 1;
  }
  if ((!defined($repopath)) && (lc $repotype ne 'vfiles'))  {
    print "Repository path or instance has to be set\n";
    exit 1;
  }
  if (!defined($dbname)) {
    print "dbname has to be set\n";
    exit 1;
  }
}

if (lc $action eq 'addlistener') {
  if (!defined($endpoint)) {
    print "At least one endpoint parameter has to be set\n";
    exit 1;
  }
}

if ((lc $action eq 'addlistener') || (lc $action eq 'deletelistener')) {
  if (!defined($listenername)) {
    print "listenername parameter has to be set\n";
    exit 1;
  }
}

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
    
    if ((lc $action eq 'enable') || (lc $action eq 'disable') || (lc $action eq 'refresh')) {

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
    
    if ( lc $action eq 'adduser' ) {
      print "Adding user to environment $env_name \n";
      if ($environments->createEnvUser($envitem, $username, $authtype, $password)) {
        print "Problem with adding user \n";
        $ret = $ret + 1;
      }
    }

    if ( lc $action eq 'deleteuser' ) {
      print "Deleting user from environment $env_name \n";
      if ($environments->deleteEnvUser($envitem, $username)) {
        print "Problem with deleting user \n";
        $ret = $ret + 1;
      }
    }

    if ( lc $action eq 'addrepo' ) {
      print "Adding repository $repopath to environment $env_name \n";
      my $repository_obj = new Repository_obj($engine_obj, $debug);
      if ($repository_obj->createRepository($envitem, $repotype, $repopath, $bits, $ohversion, $oraclebase)) {
        print "Problem with adding repository \n";
        $ret = $ret + 1;
      }
    }
    
    if ( lc $action eq 'deleterepo' ) {
      print "Deleting repository $repopath from environment $env_name \n";
      my $repository_obj = new Repository_obj($engine_obj, $debug);
      if ($repository_obj->deleteRepository($envitem, $repopath)) {
        print "Problem with adding repository \n";
        $ret = $ret + 1;
      }
    }
    
    if ( lc $action eq 'adddatabase' ) {
      
      my $repository_obj = new Repository_obj($engine_obj, $debug);
      
      my $repo;
      if (lc $repotype eq 'vfiles') {
        print "Adding vfiles $vfilepath as $dbname into environment $env_name \n";
        $repo = $repository_obj->getRepositoryByNameForEnv('Unstructured Files', $envitem);
      } else {
        print "Adding database $dbname into $repopath on environment $env_name \n";
        $repo = $repository_obj->getRepositoryByNameForEnv($repopath, $envitem);
      }
      
      if (defined($repo->{reference})) {
        my $sourceconfig_obj = new SourceConfig_obj($engine_obj, $debug);
        
        if (lc $repotype eq 'oracle') {
          if ($sourceconfig_obj->createSourceConfig('oracleSI', $repo->{reference}, $dbname, $uniquename, $instancename, $jdbc)) {
            print "Can't add database $dbname \n";
            $ret = $ret + 1;
          } else {
            print "Database $dbname added into $repopath\n";
          }          
        } elsif (lc $repotype eq 'vfiles') {
          if ($sourceconfig_obj->createSourceConfig('vfiles', $repo->{reference}, $dbname, undef, undef, undef, $vfilepath)) {
            print "Can't add directory $vfilepath as $dbname \n";
            $ret = $ret + 1;
          } else {
            print "vFiles source $vfilepath added into environment $env_name\n";
          } 
        }
        

      } else {
        print "Can't find repository path $repopath \n";
        $ret = $ret + 1;
      }
    }
    
    if ( lc $action eq 'deletedatabase' ) {
      my $repository_obj = new Repository_obj($engine_obj, $debug); 
      my $repo;
      if (lc $repotype eq 'vfiles') {
        print "Deleting vfiles $dbname from environment $env_name \n";
        $repo = $repository_obj->getRepositoryByNameForEnv('Unstructured Files', $envitem);
        $repopath = "Unstructured Files";
      } else {
        print "Deleting database $dbname from $repopath on environment $env_name \n";
        $repo = $repository_obj->getRepositoryByNameForEnv($repopath, $envitem);
      }

      if (defined($repo->{reference})) {
        my $sourceconfig_obj = new SourceConfig_obj($engine_obj, $debug);
        if ($sourceconfig_obj->deleteSourceConfig($dbname, $repo->{reference})) {
          print "Can't delete database $dbname \n";
          $ret = $ret + 1;
        } else {
          print "Database $dbname deleted from $repopath\n";
        }
      } else {
        print "Repository $repopath not found\n";
        $ret = $ret + 1;
      }
      
         
    }
    
    if ( lc $action eq 'addlistener' ) {
      print "Adding listener to environment $env_name \n";
      if ($environments->createListener($envitem, $listenername, $endpoint)) {
        print "Problem with adding listener \n";
        $ret = $ret + 1;
      }
    }

    if ( lc $action eq 'deletelistener' ) {
      print "Adding listener to environment $env_name \n";
      if ($environments->deleteListener($envitem, $listenername)) {
        print "Problem with deleting listener \n";
        $ret = $ret + 1;
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

 dx_ctl_env [ -engine|d <delphix identifier> | -all ] [ -configfile file ]
            [ -name env_name | -reference reference ]  
            -acton <enable|disable|refresh|adduser|addrepo|adddatabase|addlistener|deleteuser|deleterepo|deletedatabase|deletelistener>
            [-dbname dbname]
            [-instancename instancename]
            [-uniquename db_unique_name]
            [-jdbc jdbc_connection_string]
            [-listenername listenername]
            [-endpoint ip:port] 
            [-username name]
            [-authtype password|systemkey]
            [-password password]
            [-repotype oracle|vfiles]
            [-repopath ORACLE_HOME]
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

=item B<-configfile file>
Location of the configuration file.
A config file search order is as follow:
- configfile parameter
- DXTOOLKIT_CONF variable
- dxtools.conf from dxtoolkit location

=back

=head2 Actions

=over 1

=item B<-action> <enable|disable|refresh|adduser|addrepo|adddatabase|addlistener|deleteuser|deleterepo|deletedatabase|deletelistener>
Run an action specified for environments selected by filter or all environments deployed on Delphix Engine

=back

=head2 Filters

=over 2

=item B<-name>
Environment Name

=back

=head1 OPTIONS

=over 2

=item B<-dbname dbname>
Name of database to add (use with adddatabase)

=item B<-instancename instancename>
Name of database instance to add (use with adddatabase)

=item B<-uniquename db_unique_name>
Unique name of database to add (use with adddatabase)

=item B<-jdbc IP:PORT:SID | IP:PORT/SERVICE>
JDBC connection string (use with adddatabase)

=item B<-listenername listenername>
Listener name (use with addlistener)

=item B<-endpoint ip:port>
Listener endpoint (use with addlistener)

=item B<-username username>
Username to add (use with adduser)

=item B<-authtype password|systemkey>
Authentication type for user (use with adduser)

=item B<-password password>
Password for user (use with adduser)

=item B<-repotype oracle|vfiles>
Repository type to add (only Oracle and vFiles support for now - use with addrepo or adddatabase)

=item B<-repopath ORACLE_HOME>
Oracle Home to add (use with addrepo)

=item B<-bits 32|64>
Oracle Home binary bit version (32/64) 

=item B<-ohversion x.x.x.x>
Oracle Home version ex. 11.2.0.4 or 12.1.0.2
 
=item B<-oraclebase path>
Oracle Base path

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
 
Adding an Oracle Home not discovered automatically 

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action addrepo -repotype oracle -repopath /u01/app/oracle/121_64 -bits 64 -ohversion 12.1.0.2 -oraclebase /u01/app/oracle
 Adding repository /u01/app/oracle/121_64 to environment LINUXTARGET
 Repository /u01/app/oracle/121_64 created
 
Deleteing an Oracle Home 

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action deleterepo  -repopath /u01/app/oracle/121_64
 Deleting repository /u01/app/oracle/121_64 from environment LINUXTARGET
 Repository /u01/app/oracle/121_64 deleted
 
Adding an additional user to environment

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action adduser -username www-data -authtype password -password delphix
 Adding user to environment LINUXTARGET
 User www-data created
 
Deleting an additional user from environment 

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action deleteuser -username www-data
 Deleting user from environment LINUXTARGET
 User www-data deleted
 
Adding an additional listener called ADDLIS

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action addlistener -listenername ADDLIS -endpoint 127.0.0.1:1522
 Adding listener to environment LINUXTARGET
 Listener ADDLIS created 
 
Deleting an additional listener called ADDLIS

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action deletelistener -listenername ADDLIS
 Adding listener to environment LINUXTARGET
 Listener ADDLIS deleted
 
Adding an Oracle database rmantest into environment

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action adddatabase -dbname rmantest -instancename rmantest -uniquename rmantest -jdbc 172.16.111.222:1521:rmantest -repopath "/u01/app/oracle/12.1.0.2/rachome1"
 Adding database rmantest into /u01/app/oracle/12.1.0.2/rachome1 on environment LINUXTARGET
 Database rmantest added into /u01/app/oracle/12.1.0.2/rachome1 

Deleting an Oracle database rmantest from environment

 dx_ctl_env -d Landshark51 -name LINUXTARGET -action deletedatabase -dbname rmantest -repopath "/u01/app/oracle/12.1.0.2/rachome1"
 Deleting database rmantest from /u01/app/oracle/12.1.0.2/rachome1 on environment LINUXTARGET
 Database rmantest deleted from /u01/app/oracle/12.1.0.2/rachome1
 
Adding a vfiles into environment 

 dx_ctl_env -d Landshark51 -name LINUXSOURCE -action adddatabase -dbname swingbench -repotype vfiles -vfilepath "/home/delphix/swingbench"
 Adding vfiles /home/delphix/swingbench as swingbench into environment LINUXSOURCE
 vFiles source /home/delphix/swingbench added into environment LINUXSOURCE

Delete a vfiles from environment

 dx_ctl_env -d Landshark51 -name LINUXSOURCE -action deletedatabase -dbname swingbench -repotype vfiles
 Deleting vfiles swingbench from environment LINUXSOURCE
 Database swingbench deleted from Unstructured Files

=cut



