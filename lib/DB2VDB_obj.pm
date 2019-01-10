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
# Copyright (c) 2018,2019 by Delphix. All rights reserved.

#
# Program Name : DB2VDB_obj.pm
# Description  : Delphix Engine DB2 Database objects


# class DB2VDB_obj - is a child class of VDB_obj

package DB2VDB_obj;
use Data::Dumper;
use JSON;
use version;
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering DB2VDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug);


    my @configureClone;
    my @postRefresh;
    my @preRefresh;
    my @configParams;
    my @mntPoints;
    my %configParams = ();

    my %operations = (
        "type" => "VirtualSourceOperations",
        "configureClone" => \@configureClone,
        "postRefresh" => \@postRefresh,
        "preRefresh" => \@preRefresh
    );
    my %prov = (
        "type" => "AppDataProvisionParameters",
        "container" => {
            "type" => 'AppDataContainer',
            "name" => '',
            "group" => ''
        },
        "sourceConfig" => {
                "type" => "AppDataDirectSourceConfig",
                "repository" => "",
                "name" => "",
                "path" => ""
                #"parameters" => \%configParams
        },
        "source" => {
                "type" => "AppDataVirtualSource",
                #"additionalMountPoints" => \@mntPoints,
                #"operations" => \%operations#,
                #"parameters" => \%configParams
        },
        "timeflowPointParameters" => {
            "type" => "TimeflowPointSemantic",
            "container" => "",
            "location" => "LATEST_SNAPSHOT"
        },
    );
    $self->{"NEWDB"} = \%prov;
    $self->{_dbtype} = 'db2';
    return $self;
}



# Procedure getdSourceBackup`
# parameters:
# -engine
# -output
# -backup - location for hooks
# -groupname

# Return a definition of backup metadata

sub getdSourceBackup
{
    my $self = shift;
    my $engine = shift;
    my $output = shift;
    my $backup = shift;
    my $groupname = shift;

    logger($self->{_debug}, "Entering DB2VDB_obj::getdSourceBackup",1);

    my $suffix = '';
    if ( $^O eq 'MSWin32' ) {
      $suffix = '.exe';
    }

    $self->exportDBHooks($backup);

    my $dbtype = $self->getType();
    my $dbn = $self->getName();
    my $dbhostname;
    my $vendor = $self->{_dbtype};
    my $rephome = $self->getHome();

    $dbhostname = $self->getSourceConfigName();

    if (! defined($dbhostname)) {
      $dbhostname = 'detached';
    }

    my $osuser = $self->getOSUser();

    $restore_args = "dx_ctl_dsource$suffix -d $engine -action create -group \"$groupname\" -creategroup ";
    $restore_args = $restore_args . "-dsourcename \"$dbn\"  -type $vendor -sourcename \"$dbhostname\" ";
    $restore_args = $restore_args . "-sourceinst \"$rephome\" -sourceenv \"" . $self->getEnvironmentName() . "\" -source_os_user \"$osuser\" ";

    $output->addLine(
      $restore_args
    );

}



# Procedure getVDBBackup`
# parameters:
# -engine
# -output
# -backup - location for hooks
# -groupname
# -parentname
# -parentgroup
# -templates - handler to template object
# Return a definition of backup metadata

sub getVDBBackup
{
    my $self = shift;
    my $engine = shift;
    my $output = shift;
    my $backup = shift;
    my $groupname = shift;
    my $parentname = shift;
    my $parentgroup = shift;
    my $templates = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::getVDBBackup",1);

    my $suffix = '';
    if ( $^O eq 'MSWin32' ) {
      $suffix = '.exe';
    }

    my $dbtype = $self->getType();
    my $dbn = $self->getName();
    my $dbhostname;
    my $vendor = $self->{_dbtype};
    my $rephome = $self->getHome();

    $self->exportDBHooks($backup);

    my $restore_args;

    $dbhostname = $self->getDatabaseName();

    if ($parentname eq '') {
      $restore_args = "dx_provision_vdb$suffix -d $engine -type $vendor -group \"$groupname\" -creategroup -empty  ";
    } else {
      $restore_args = "dx_provision_vdb$suffix -d $engine -type $vendor -group \"$groupname\" -creategroup -sourcename \"$parentname\"  -srcgroup \"$parentgroup\" ";
    }

    $restore_args = $restore_args . " -targetname \"$dbn\" ";
    $restore_args = $restore_args . " -dbname \"$dbhostname\" -environment \"" . $self->getEnvironmentName() . "\" ";
    $restore_args = $restore_args . " -envinst \"$rephome\" ";


    $restore_args = $restore_args . " -envUser \"" . $self->getEnvironmentUserName() . "\" ";
    $restore_args = $restore_args . " -hooks " . File::Spec->catfile($backup,$dbn.'.dbhooks') . " ";

    $restore_args = $restore_args . $self->getConfig();

    $output->addLine(
      $restore_args
    );

}

# Procedure getConfig
# parameters: none
# Return database config

sub getConfig
{
    my $self = shift;
    my $templates = shift;
    my $backup = shift;

    logger($self->{_debug}, "Entering DB2VDB_obj::getConfig",1);
    my $config = '';
    my $joinsep;

    if (defined($backup)) {
      $joinsep = ' ';
    } else {
      $joinsep = ',';
    }

    if ($self->getType() eq 'VDB') {
      my $addmount = $self->getAdditionalMountpoints();
      for my $am (@{$addmount}) {
        $config = join($joinsep,($config, "-additionalMount $am "));
      }
    }

    if ( (my $rest) = $config =~ /^,(.*)/ ) {
      $config = $rest;
    }

    return $config;

}



# Procedure snapshot
# parameters:
# - resync - yes/no
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot
{
    my $self = shift;
    my $resync = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::snapshot",1);

    my %snapshot_type = (
            "type" => "AppDataSyncParameters"
    );

    if (defined ($resync) ) {
        $snapshot_type{"resync"} = JSON::true;
    };

    return $self->VDB_obj::snapshot(\%snapshot_type) ;
}

# Procedure setEmpty
# parameters:
# set a flag to create a empty vFiles

sub setEmpty {
    my $self = shift;

    my $group = shift;
    my $env = shift;
    my $inst = shift;
    my $mountpoint = shift;

    logger($self->{_debug}, "Entering DB2VDB_obj::setEmpty",1);
    $self->{_empty} = 1;
}

# Procedure setEnvironment
# parameters:
# - name - environment name
# - envUser - user name
# Set environment reference by name for new db
# Return 0 if success, 1 if not found

sub setEnvironment {
    my $self = shift;
    my $name = shift;
    my $envUser = shift;
    logger($self->{_debug}, "Entering VDB_obj::setEnvironment",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};

    my $environments;
    if (defined($self->{_environment})) {
        $environments = $self->{_environment};
    } else {
        $environments = new Environment_obj($dlpxObject, $debug);
        $self->{_environment} = $environments;
    }

    my $envitem = $environments->getEnvironmentByName($name);

    if (defined ($envitem)) {
        $self->{'_newenv'} = $envitem->{'reference'};
        $self->{'_hosts'} = $envitem->{'host'};
        $self->{'_newenvtype'} = $envitem->{'type'};

        if (defined($envUser)) {
          my $envUser_ref = $environments->getEnvironmentUserByName($envitem->{'reference'}, $envUser);
          if (defined($envUser_ref)) {
            $self->{NEWDB}->{sourceConfig}->{environmentUser} = $envUser_ref;
          } else {
            print "Environment user $envUser not found in environment $name.\n";
            return 1;
          }
        }
        return 0;
    } else {
        return 1;
    }

}


# Procedure createVDB
# parameters:
# - group - new DB group
# - env - new DB environment
# - inst - new DB instance
# Start job to create vFiles VBD
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef

sub createVDB {
    my $self = shift;

    my $group = shift;
    my $env = shift;
    my $inst = shift;
    my $mountpoint = shift;

    logger($self->{_debug}, "Entering DB2VDB_obj::createVDB",1);


    if ( $self->setGroup($group) ) {
        print "Group $group not found. VDB won't be created\n";
        return undef;
    }

    if ( $self->setEnvironment($env) ) {
        print "Environment $env not found. VDB won't be created\n";
        return undef;
    }

    if ( $self->setHome($inst) ) {
        print "Instance $inst in environment $env not found. VDB won't be created\n";
        return undef;
    }

    if ( ! defined($self->{"NEWDB"}->{"container"}->{"name"} ) ) {
        print "Set name using setName procedure before calling create VDB. VDB won't be created\n";
        return undef;
    }


    my $operation = 'resources/json/delphix/database/provision';

    if (defined($self->{_empty})) {
      delete $self->{NEWDB}->{timeflowPointParameters};
      $self->{NEWDB}->{type} = "DB2EmptyVFilesCreationParameters";
      $operation  = 'resources/json/delphix/database/createEmpty';
    }


    my $json_data = $self->getJSON();

    return $self->runJobOperation($operation,$json_data);

}

# Procedure addSource
# parameters:
# - source - name of source DB
# - source_inst - instance
# - source_env - env
# - source_osuser - name of source OS user
# - dsource_name - name of dsource in environment
# - group - dsource  group
# Start job to add DB2 dSource
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef

sub addSource {
    my $self = shift;
    my $source = shift;
    my $source_inst = shift;
    my $source_env = shift;
    my $source_osuser = shift;
    my $dbuser = shift;
    my $password = shift;
    my $dsource_name = shift;
    my $group = shift;
    my $logsync = shift;
    my $env = shift;
    my $inst = shift;
    my $stage_osuser = shift;
    my $backup_dir = shift;
    my $dumppwd = shift;
    my $validatedSyncMode = shift;
    my $delphixmanaged = shift;
    my $compression = shift;

    logger($self->{_debug}, "Entering DB2VDB_obj::addSource",1);
    print Dumper "dupa";

    my $config = $self->setConfig($source, $inst, $env);

    if (! defined($config)) {
        print "Source database $source not found\n";
        return undef;
    }

    print Dumper "config";
    print Dumper $dsource_name;
    print Dumper $source;
    print Dumper $config->{reference};

    if ( $self->setEnvironment($env) ) {
        print "Staging environment $env not found. dSource won't be created\n";
        return undef;
    }

    if ( $self->setHome($inst) ) {
        print "Staging instance $inst in environment $env not found. dSource won't be created\n";
        return undef;
    }

    my $stage_osuser_ref = $self->{_environment}->getEnvironmentUserByName($env,$stage_osuser);

    if (!defined($stage_osuser_ref)) {
        print "Source OS user $stage_osuser not found\n";
        return undef;
    }

    print Dumper $self->{'_newenv'};
    print Dumper $stage_osuser_ref;
    print Dumper $backup_dir;

    my @configset;

    my %dsource_params = (
      "type" => "LinkParameters",
      "group" => $self->{"NEWDB"}->{"container"}->{"group"},
      "name" => $dsource_name,
      "linkData" => {
          "type" => "AppDataStagedLinkData",
          "config" => $config->{reference},
          "environmentUser" => $source_os_ref,
          "stagingEnvironment" => $self->{'_newenv'},
          "stagingEnvironmentUser" => $stage_osuser_ref,
          "parameters" => {
              "monitorHADR" => JSON::false,
              "toolkitHookFlag" => JSON::false,
              "config_settings_stg" => \@configset,
              "dbName" => $source,
              "backupPath" => $backup_dir
          }
      }
    );

    print Dumper \%dsource_params;

    exit;

    my $operation = 'resources/json/delphix/database/link';
    my $json_data = to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);

    logger($self->{_debug}, $json_data, 1);

    return $self->runJobOperation($operation,$json_data, 'ACTION');

  }

  # === POST /resources/json/delphix/database/link ===
  # {
  #     "type": "LinkParameters",
  #     "name": "R74D105E",
  #     "group": "GROUP-1",
  #     "linkData": {
  #         "type": "AppDataStagedLinkData",
  #         "config": "APPDATA_STAGED_SOURCE_CONFIG-13",
  #         "environmentUser": "HOST_USER-10",
  #         "parameters": {
  #             "monitorHADR": false,
  #             "toolkitHookFlag": false,
  #             "config_settings_stg": [],
  #             "dbName": "R74D105E",
  #             "backupPath": "/db2backup"
  #         },
  #         "stagingEnvironment": "UNIX_HOST_ENVIRONMENT-7",
  #         "stagingEnvironmentUser": "HOST_USER-10"
  #     }
  # }

# Procedure setName
# parameters:
# - contname - container name
# - dbname - database name
# Set name for new db.

sub setName {
    my $self = shift;
    my $contname = shift;
    my $dbname = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::setName",1);

    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    #$self->{"NEWDB"}->{"source"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"name"} = "";#$dbname;
    $self->{"NEWDB"}->{"source"}->{"parameters"}->{"databaseAliasName"} = $dbname;
    #$self->{"NEWDB"}->{"source"}->{"databaseAliasName"} = $dbname;
    #$self->{"NEWDB"}->{"parameters"}->{"databaseAliasName"} = $dbname;
    #$self->{"NEWDB"}->{"databaseAliasName"} = $dbname;

}


# Procedure getAdditionalMountpoints
# parameters: none
# Return an array with combained list of additional mount point env,path,sharedPath

sub getAdditionalMountpoints
{
    my $self = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::getAdditionalMountpoints",1);

    my @retarray;
    my $addmountarray = $self->{source}->{additionalMountPoints};

    #print Dumper $addmountarray;

    for my $addmount (@{$addmountarray}) {
      my $envname = $self->{_environment}->getName($addmount->{environment});
      if (defined($envname)) {
        my $addstring = "\"$envname\",\"" . $addmount->{mountPath} . "\",\"" . $addmount->{sharedPath} . "\"";
        push(@retarray, $addstring);
      } else {
        next;
      }
    }

    return \@retarray;
}

# Procedure setAdditionalMountpoints
# parameters:
# - array of mountpoints
# Return an array with combained list of additional mount point env,path,sharedPath

sub setAdditionalMountpoints
{
    my $self = shift;
    my $addmount = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::setAdditionalMountpoints",1);
    my @additionalMountPoints;
    for my $ap (@{$addmount}) {
      my ($env, $path, $shared) = split(',', $ap);
      my $env_obj = $self->{_environment}->getEnvironmentByName($env);
      if (!defined($env_obj)) {
        print "Environment for additional mount point not found\n";
        return 1;
      }
      if (!defined($path)) {
        print "Additional path for additional mount point not found\n";
        return 1;
      }
      if (!defined($shared)) {
        print "Shared path for additional mount point not found\n";
        return 1;
      }
      my %addmount_hash =  (
        'type' => 'DB2AdditionalMountPoint',
        'mountPath' => $path,
        'sharedPath' => $shared,
        'environment' => $env_obj->{reference}
      );

      push (@additionalMountPoints, \%addmount_hash);

    }

    $self->{NEWDB}->{source}->{additionalMountPoints} = \@additionalMountPoints;
    return 0;
}

sub setMountPoint {
    my $self = shift;
    my $mountpoint = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::setMountPoint",1);
    $self->{"NEWDB"}->{"source"}->{"mountBase"} = $mountpoint;
}


# Procedure getDatabaseName
# parameters: none
# Return database name

sub getDatabaseName
{
    my $self = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::getDatabaseName",1);
    return $self->{sourceConfig}->{path};
}

# Procedure setSource
# parameters:
# - name - source name
# Set dsource reference by name for new db.
# Return 0 if success, 1 if not found

sub setSource {
    my $self = shift;
    #my $name = shift;
    my $sourceitem = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};


    if (defined ($sourceitem)) {
        my $sourcetype = $sourceitem->{container}->{'type'};

        if (($sourcetype eq 'AppDataContainer') || ($sourcetype eq 'AppDataVirtualSource') || ($sourcetype eq 'AppDataLinkedSource') ) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}  = $sourceitem->{container}->{reference};
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }

}

# Procedure getVersion
# parameters:
# Return db version

sub getVersion {
    my $self = shift;
    logger($self->{_debug}, "Entering DB2VDB_obj::getVersion",1);

    my $version;
    if ( defined($self->{"repository"}) && ( $self->{"repository"} ne 'NA' ) ) {
        if  (defined($self->{"repository"}->{parameters}) && defined($self->{"repository"}->{parameters}->{version})) {
          $version = $self->{"repository"}->{parameters}->{version};
        } else {
          $version = 'N/A';
        }
    } else {
        $version = 'N/A';
    }

    return $version;

}

1;
