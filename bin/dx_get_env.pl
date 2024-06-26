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
# Program Name : dx_get_env.pl
# Description  : Get database and host information
# Author       : Edward de los Santos
# Created      : 30 Jan 2014 (v1.0.0)
#
#
# Modified: 14 Mar 2015 (v2.0.0) Marcin Przepiorowski
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
use Host_obj;
use SourceConfig_obj;
use Databases;

my $version = $Toolkit_helpers::version;

GetOptions(
  'help|?' => \(my $help),
  'd|engine=s' => \(my $dx_host),
  'name|n=s' => \(my $envname),
  'reference|r=s' => \(my $reference),
  'userlist' => \(my $userlist),
  'config' => \(my $config),
  'cluster' => \(my $cluster),
  'backup=s' => \(my $backup),
  'replist' => \(my $replist),
  'sourcelist' => \(my $sourcelist),
  'dbname=s' => \(my $dbname),
  'nohead' => \(my $nohead),
  'format=s' => \(my $format),
  'debug:i' => \(my $debug),
  'save=s' => \(my $save),
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

if (defined($dbname) && (!defined($envname))) {
  print "Option -dbname requires environemnt name -name to be specified \n";
  pod2usage(-verbose => 1,  -input=>\*DATA);
  exit (1);
}

# this array will have all engines to go through (if -d is specified it will be only one engine)
my $engine_list = Toolkit_helpers::get_engine_list($all, $dx_host, $engine_obj);

my $output = new Formater();

if (defined($backup)) {
  if (! -d $backup) {
    print "Path $backup is not a directory \n";
    exit (1);
  }
  if (! -w $backup) {
    print "Path $backup is not writtable \n";
    exit (1);
  }
}

if (defined($userlist)) {
  $output->addHeader(
    {'Appliance', 20},
    {'Environment Name',   30},
    {'User name'       ,   30},
    {'Auth Type',          30}
  );
} elsif (defined($replist)) {
  $output->addHeader(
    {'Appliance', 20},
    {'Environment Name',  30},
    {'Repository list',   30}
  );
} elsif (defined($sourcelist)) {
  $output->addHeader(
    {'Appliance', 20},
    {'Environment Name',  30},
    {'Repository',        30},
    {'DB name',           30},
    {'DB type',           30},
    {'dSource/VDB',       10},
    {'dSource/VDB name',  30}
  );
}elsif (defined($config)) {
  $output->addHeader(
    {'Appliance',         20},
    {'Environment Name',  30},
    {'Type',              25},
    {'Host name',         30},
    {'User Name',         30},
    {'Auth Type',         30},
    {'Config'   ,         60}
  );
} elsif (defined($backup)) {
  $output->addHeader(
    {'Command',        200}
  )
} elsif (defined($cluster)) {
  $output->addHeader(
    {'Appliance', 20},
    {'Environment Name',  30},
    {'Type',      25},
    {'Status',     8},
    {'OS Version', 50},
    {'Hostname',  50}
  );
}
else {
  $output->addHeader(
    {'Appliance', 20},
    {'Environment Name',  30},
    {'Type',      25},
    {'Status',     8},
    {'OS Version', 50}
  );
}


my %save_state;

my $ret = 0;

for my $engine ( sort (@{$engine_list}) ) {
  # main loop for all work
  if ($engine_obj->dlpx_connect($engine)) {
    print "Can't connect to Dephix Engine $dx_host\n\n";
    $ret = $ret + 1;
    next;
  };

  # load objects for current engine
  my $environments;
  my $repository_obj;
  my $host_obj;
  my $toolkits;
  my $sourceconfigs;
  my $databases;

  if (defined($sourcelist)) {
    $toolkits = new Toolkit_obj($engine_obj, $debug);
    $databases = new Databases( $engine_obj, $debug );
    $sourceconfigs = $databases->{_sourceconfigs};
    $environments = $databases->{_environments};
    $repository_obj = $databases->{_repositories};
  } else {
    $environments = new Environment_obj( $engine_obj, $debug);
    $repository_obj = new Repository_obj($engine_obj, $debug);
    $host_obj = new Host_obj ( $engine_obj, $debug );
  }

  # filter implementation

  my @env_list;

  if (defined($reference)) {
    push(@env_list, $reference);
  }
  elsif (defined($envname)) {
    if (defined($environments->getEnvironmentByName($envname))) {
      push(@env_list, $environments->getEnvironmentByName($envname)->{reference});
    } else {
      $ret = $ret + 1;
    }
  } else {
    @env_list = $environments->getAllEnvironments();
  };

  if (defined($backup)) {
    # backup of environments should process list in creation order
    # assumptions - environment no sequence is growing
    # so sorting on that should solve dependency problem
    @env_list = sort { Toolkit_helpers::sort_by_number($a, $b) } @env_list;
  }

  # for filtered databases on current engine - display status
  for my $envitem ( @env_list ) {

    if (defined($userlist)) {
      print_users($output, $engine, $environments, $envitem);
    } elsif (defined($replist)) {
      print_repo($output, $engine, $environments, $envitem, $repository_obj);
    } elsif (defined($sourcelist)) {
      $ret = $ret + print_source($output, $engine, $envitem, $databases, $toolkits, $dbname);
    } elsif (defined($config) || defined($backup)) {
      gen_backup_config($output, $engine, $environments, $envitem, $backup, $host_obj);
    } else {
      my $cluster_nodes;
      my $host_ref = $environments->getHost($envitem);
      my $hostos;
      if (($host_ref ne 'CLUSTER') && ($host_ref ne 'NA')) {
        $hostos = $host_obj->getOSVersion($host_ref);
        $cluster_nodes =  $host_obj->getHostAddr($host_ref);
      } else {
        my $clusenvnode = $environments->getClusterNode($envitem);
        $host_ref = $environments->getHost($clusenvnode);
        $hostos = $host_obj->getOSVersion($host_ref);
        if ($cluster) {
          $cluster_nodes = join(";", map { $host_obj->getHostAddr($environments->getHost($_)) } @{$environments->getClusterNodes($envitem)});
        }
      }

      if ($cluster) {
        $output->addLine(
          $engine,
          $environments->getName($envitem),
          $environments->getType($envitem),
          $environments->getStatus($envitem),
          $hostos,
          $cluster_nodes
        );
      } else {
        $output->addLine(
          $engine,
          $environments->getName($envitem),
          $environments->getType($envitem),
          $environments->getStatus($envitem),
          $hostos
        );
      }
    }

    $save_state{$envitem} = $environments->getStatus($envitem);

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
  my $filename = File::Spec->catfile($backup,'backup_env.txt');

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

sub print_users {
  my $output = shift;
  my $engine = shift;
  my $environments = shift;
  my $envitem = shift;

  $output->addLine(
    $engine,
    $environments->getName($envitem),
    '*' . $environments->getPrimaryUserName($envitem),
    $environments->getPrimaryUserAuth($envitem)
  );
  for my $useritem (@{$environments->getEnvironmentNotPrimaryUsers($envitem)}) {
    $output->addLine(
      '',
      '',
      $environments->getEnvironmentUserNamebyRef($envitem,$useritem),
      $environments->getEnvironmentUserAuth($envitem,$useritem)
    );
  }
}

sub print_repo {
  my $output = shift;
  my $engine = shift;
  my $environments = shift;
  my $envitem = shift;
  my $repository_obj = shift;

  $output->addLine(
    $engine,
    $environments->getName($envitem),
    ''
  );
  my $reparray = $repository_obj->getRepositoryByEnv($envitem);
  for my $repitem (@{$reparray}) {
    $output->addLine(
      '',
      '',
      $repository_obj->getName($repitem)
    );
  }
}
  
sub print_source {
  my $output = shift;
  my $engine = shift;
  my $envitem = shift;
  my $databases = shift;
  my $toolkits = shift;
  my $filter_db_name = shift;

  my $sourceconfigs = $databases->{_sourceconfigs};
  my $environments = $databases->{_environments};
  my $repository_obj = $databases->{_repositories};
  my $sources = $databases->{_source};

  my $envname = $environments->getName($envitem);


  my $rdbms;
  my $sourceconfig;
  my $source;
  my $dbobj;
  my $reponame;
  my $dbname;
  my $dbtype;
  my $delphixname;
  my $ret = 0;

  my $reparray = $repository_obj->getRepositoryByEnv($envitem);
  for my $repitem (@{$reparray}) {
    $reponame = $repository_obj->getName($repitem);
    my $dbarray = $sourceconfigs->getSourceConfigsListForRepo($repitem);

    my @outputdbarray;
    my $outputdbarray_pointer;
    if (defined($filter_db_name)) {
      for my $dbitem (@{$dbarray}) {
        $sourceconfig = $sourceconfigs->getSourceConfig($dbitem);
        if (defined($sourceconfig->{'toolkit'})) {
          $dbname = $sourceconfigs->getName($dbitem);
        } else {
          $dbname = $sourceconfigs->getSourceConfig($dbitem)->{'databaseName'};
        }
        if ($dbname eq $filter_db_name) {
          push(@outputdbarray, $dbitem);
        }
      }
      if (scalar(@outputdbarray)>0) {
        $ret = 0;
      } else {
        $ret = 1;
      }
      $outputdbarray_pointer = \@outputdbarray;

    } else {
      $outputdbarray_pointer = $dbarray;
    }

    for my $dbitem (@{$outputdbarray_pointer}) {
      # print Dumper "databases";
      # print Dumper $dbitem;
      # print Dumper $sourceconfigs->getName($dbitem);
      # print Dumper $sourceconfigs->getSourceConfig($dbitem)->{'databaseName'};

      # $dbname = 'slon';

      $sourceconfig = $sourceconfigs->getSourceConfig($dbitem);
      if (defined($sourceconfig->{'toolkit'})) {
        if ($toolkits->getName($sourceconfig->{'toolkit'}) eq 'postgres-vsdk') {
          $rdbms = 'postgresql';
        } elsif ($toolkits->getName($sourceconfig->{'toolkit'}) eq 'db2db') {
          $rdbms = 'db2';
        } else {
          $rdbms = 'unknown';
        }
        $dbname = $sourceconfigs->getName($dbitem);
      } else {
        $dbname = $sourceconfigs->getSourceConfig($dbitem)->{'databaseName'};
        if ($dbitem =~ /^MSSQL/) {
          $rdbms = "mssql";
        } elsif ($dbitem =~ /^ORACLE/) {
          if (defined($sourceconfig->{'cdbType'})) {
            if ($sourceconfig->{'cdbType'} eq 'NON_CDB') {
              $rdbms = "oracle";
            } elsif ($sourceconfig->{'cdbType'} eq 'ROOT_CDB') {
              $rdbms = "oracle CDB";
            } 
          } elsif (defined($sourceconfig->{'cdbConfig'})) {
            $rdbms = "oracle PDB";
          } else {
            $rdbms = "unknown oracle";
          }
        } elsif ($dbitem =~ /^ASE/) {
          $rdbms = "sybase";
        }
      }

      $source = $sources->getSourceByConfig($dbitem);
      if (defined($source)) {
        $dbobj = $databases->getDB($source->{'container'});
        $delphixname = $dbobj->getName();
        $dbtype = $dbobj->getType();
      } else {
        # not ingested
        $dbtype = '';
        $delphixname = 'not ingested';
      }


      
      $output->addLine(
        $engine,
        $envname,
        $reponame,
        $dbname,
        $rdbms,
        $dbtype,
        $delphixname
      )
    }
  }

  return $ret;
}

sub gen_backup_config {
  my $output = shift;
  my $engine = shift;
  my $environments = shift;
  my $envitem = shift;
  my $backup = shift;
  my $host_obj = shift;

  my $envtype = $environments->getType($envitem);
  my $host_ref = $environments->getHost($envitem);
  my $envname = $environments->getName($envitem);
  my $userauth = $environments->getPrimaryUserAuth($envitem);
  my $hostname;
  my $user = $environments->getPrimaryUserName($envitem);

  if (($host_ref ne 'CLUSTER') && ($host_ref ne 'NA')) {
    $hostname = $host_obj->getHostAddr($host_ref);
  } else {
    my $clusenvnode = $environments->getClusterNode($envitem);
    $host_ref = $environments->getHost($clusenvnode);
    $hostname = $host_obj->getHostAddr($host_ref);
  }



  if (defined($backup)) {

    my $backup = $environments->getBackup($envitem, $host_obj, $engine, $envname, $envtype, $hostname, $user, $userauth);
    $output->addLine(
      $backup
    );

    #add users

    $environments->getUsersBackup($envitem,$output,$engine);



  } else {

    $config = $environments->getConfig($envitem, $host_obj);
    $output->addLine(
      $engine,
      $envname,
      $envtype,
      $hostname,
      $user,
      $userauth,
      $config
    );

  }
}
__DATA__

=head1 SYNOPSIS

 dx_get_env [-engine|d <delphix identifier> | -all ]
            [-name env_name | -reference reference ]
            [-backup]
            [-replist ]
            [-userlist ]
            [-cluster ]
            [-format csv|json ]
            [-help|? ]
            [-debug ]

=head1 DESCRIPTION

Get the information about host environment.

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

=over 4

=item B<-name>
Environment Name

=item B<-reference>
Environment reference


=back

=head1 OPTIONS

=over 3

=item B<-backup>
Display dxtoolkit commands to recreate environments ( support for SI Oracle / MS SQL )


=item B<-replist>
Display repository list (Orcle Home / MS SQL instance / etc ) for environment

=item B<-cluster>
Print ; segrageted list of cluster nodes from the environment into a new column called cluster nodes


=item B<-format>
Display output in csv or json format
If not specified pretty formatting is used.

=item B<-help>
Print this screen

=item B<-debug>
Turn on debugging

=back

=head1 EXAMPLES

Display all environments

 dx_get_env -d Landshark

 Appliance            Reference                      Environment Name               Type                      Status
 -------------------- ------------------------------ ------------------------------ ------------------------- --------
 Landshark5           ORACLE_CLUSTER-11              racattack-cl                   rac                       enabled
 Landshark5           UNIX_HOST_ENVIRONMENT-1        LINUXTARGET                    unix                      enabled
 Landshark5           UNIX_HOST_ENVIRONMENT-44       LINUXSOURCE                    unix                      enabled
 Landshark5           WINDOWS_HOST_ENVIRONMENT-48    WINDOWSTARGET                  windows                   enabled
 Landshark5           WINDOWS_HOST_ENVIRONMENT-49    WINDOWSSOURCE                  windows                   enabled

Display all environments with repositories list

 dx_get_env -d Landshark -replist

 Appliance            Environment Name               Repository list
 -------------------- ------------------------------ ------------------------------
 Landshark            racattack
                                                     /u01/app/oracle/11.2.0.4/racho
 Landshark            LINUXTARGET
                                                     agilemasking
                                                     LINUXTARGET
                                                     /u01/app/oracle/product/11.2.0
 Landshark            LINUXSOURCE
                                                     webapp
                                                     agilemasking
                                                     LINUXSOURCE
                                                     /u01/app/oracle/product/11.2.0
 Landshark            envtest
                                                     /u01/app/oracle/11.2.0.4/db1
 Landshark            WINDOWSSOURCE
                                                     MSSQLSERVER
 Landshark            WINDOWSTARGET
                                                     MSSQLSERVER
                                                     MSSQL2012



=cut
