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
# Copyright (c) 2015,2017 by Delphix. All rights reserved.
#
# Program Name : OracleVDB_obj.pm
# Description  : Delphix Engine Database objects
# It's include the following classes:
# - SybaseVDB_obj - Sybase VDB
#
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#

# class SybaseVDB_obj - is a child class of VDB_obj

package SybaseVDB_obj;
use Data::Dumper;
use JSON;
use version;
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering SybaseVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug);

    my @configureClone;
    my @postRefresh;
    my @preRefresh;

    my %operations = (
        "type" => "VirtualSourceOperations",
        "configureClone" => \@configureClone,
        "postRefresh" => \@postRefresh,
        "preRefresh" => \@preRefresh
    );

   # Sybase specific properties

    my %prov = (
            "type" => "ASEProvisionParameters",
            "truncateLogOnCheckpoint" => JSON::false,
            "container" => {
                "type" => 'ASEDBContainer',
                "name" => '',
                "group" => '',
                #"masked" => JSON::false,
                #performanceMode" => JSON::false,
                "sourcingPolicy" => {
                    "type" => 'SourcingPolicy',
                    "loadFromBackup" => JSON::false,
                    "logsyncEnabled" => JSON::false
                }
            },
            "sourceConfig" => {
                    "type" => "ASESIConfig",
                    "repository" => "",
                    "databaseName" => "",
                    "instance" => {
                        "type" => "ASEInstanceConfig"
                    }
            },
            "source" => {
                    "type" => "ASEVirtualSource",
                    "operations" => \%operations
            },
            "timeflowPointParameters" => {
                "type" => "TimeflowPointSemantic",
                "container" => "",
                "location" => "LATEST_SNAPSHOT"
            },
    );

    $self->{"NEWDB"} = \%prov;
    $self->{_dbtype} = 'sybase';

    return $self;
}

# Procedure getBackupPath
# parameters:
# Return backup path

sub getBackupPath {
    my $self = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::loadBackupPath",1);
    return $self->{source}->{loadBackupPath};
}

# Procedure setBackupPath
# parameters:
# - source - source hash
# - path - path to set
# Return backup path

sub setBackupPath {
    my $self = shift;
    my $sourcehash = shift;
    my $path = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::setBackupPath",1);
    $sourcehash->{loadBackupPath} = $path;

}

# Procedure getLogSync
# parameters: none
# Return status of Log Sync

sub getLogSync
{
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::getLogSync",1);
    return $self->{container}->{runtime}->{logSyncActive} ? 'ACTIVE' : 'INACTIVE';
}


# Procedure getValidatedModeState
# parameters: none
# Return status of Validated Sync

sub getValidatedModeState
{
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::getValidatedModeState",1);
    my $ret;
    if (defined($self->{container}->{runtime}) && defined($self->{container}->{runtime}->{preProvisioningStatus})) {
      $ret = $self->{container}->{runtime}->{preProvisioningStatus}->{preProvisioningState};
    } else {
      $ret = "N/A";
    }
    return $ret;
}

# Procedure getValidatedModeStatus
# parameters: none
# Return status of Validated Sync

sub getValidatedModeStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::getValidatedModeStatus",1);
    my $ret;
    if (defined($self->{container}->{runtime}) && defined($self->{container}->{runtime}->{preProvisioningStatus})) {
      $ret = $self->{container}->{runtime}->{preProvisioningStatus}->{status};
    } else {
      $ret = "N/A";
    }
    return $ret;
}

# Procedure getValidatedModeAction
# parameters: none
# Return status of Validated Sync

sub getValidatedModeAction
{
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::getValidatedModeAction",1);
    my $ret;
    if (defined($self->{container}->{runtime}) && defined($self->{container}->{runtime}->{preProvisioningStatus}) && defined($self->{container}->{runtime}->{preProvisioningStatus}->{pendingAction})) {
      $ret = $self->{container}->{runtime}->{preProvisioningStatus}->{pendingAction};
    } else {
      $ret = "N/A";
    }
    return $ret;
}

# Procedure getValidatedModeUpdate
# parameters: none
# Return status of Validated Sync

sub getValidatedModeUpdate
{
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::getValidatedModeUpdate",1);
    my $ret;
    if (defined($self->{container}->{runtime}) && defined($self->{container}->{runtime}->{preProvisioningStatus})) {
      $ret = Toolkit_helpers::convert_from_utc($self->{container}->{runtime}->{preProvisioningStatus}->{lastUpdateTimestamp},
                                               $self->{_dlpxObject}->getTimezone(),1,undef);
    } else {
      $ret = "N/A";
    }
    return $ret;
}


# Procedure setLogSync
# parameters:
# - logsync - yes/no
# Enable of Log Sync

sub setLogSync
{
    my $self = shift;
    my $logsync = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::setLogSync",1);


    my $type = $self->{container}->{type};

    my %logsynchash = (
        "type"=> $type,
        "sourcingPolicy"=> {
            "type"=> "SourcingPolicy"
        }
    );

    if (lc $logsync eq 'yes') {
        $logsynchash{"sourcingPolicy"}{"logsyncEnabled"} = JSON::true;
    } else {
        $logsynchash{"sourcingPolicy"}{"logsyncEnabled"} = JSON::false;
    }


    my $ref = $self->{container}->{reference};

    my $operation = "resources/json/delphix/database/" . $ref;
    my $payload = to_json(\%logsynchash);

    return $self->runJobOperation($operation,$payload,'ACTION');
}

# Procedure getValidatedMode
# parameters:
# source - source hash



sub getValidatedMode {
    my $self = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::getValidatedMode",1);
    return $self->{source}->{validatedSyncMode} ;

}


# Procedure setValidatedMode
# parameters:
# source - source hash
# vsm - value of vsm


sub setValidatedMode {
    my $self = shift;
    my $source = shift;
    my $vsm = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::setValidatedMode",1);
    my $ret;

    $vsm = uc $vsm;

    my %vsmvalid = (
      'ENABLED'=>1,
      'DISABLED'=>1
    );

    if (!defined($vsmvalid{$vsm})) {
      print "Validated sync mode is invalid for Sybase\n";
      return 1;
    }


    $source->{validatedSyncMode} = $vsm;

    return 0;

}

# Procedure getConfig
# parameters: none
# Return database config

sub getConfig
{
    my $self = shift;
    my $templates = shift;
    my $backup = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::getConfig",1);
    my $config = '';
    my $joinsep;

    if (defined($backup)) {
      $joinsep = ' ';
    } else {
      $joinsep = ',';
    }

    if ($self->getType() eq 'VDB') {
      if ($self->getLogTruncate() eq 'enabled') {
        $config = join($joinsep,($config, "-truncateLogOnCheckpoint"));
      }
    } elsif ($self->getType() eq 'dSource') {
      my $staging_user = $self->getStagingUser();
      my $staging_env = $self->getStagingEnvironmentName();
      my $staging_inst = $self->getStagingInst();

      $config = join($joinsep,($config, "-stageinst \"$staging_inst\""));
      $config = join($joinsep,($config, "-stageenv \"$staging_env\""));
      $config = join($joinsep,($config, "-stage_os_user \"$staging_user\""));

      my $backup_path = $self->getBackupPath();
      $config = join($joinsep,($config, "-backup_dir \"$backup_path\""));
    }

    if ( (my $rest) = $config =~ /^,(.*)/ ) {
      $config = $rest;
    }

    return $config;

}


# Procedure addSource
# parameters:
# - source - name of source DB
# - source_osuser - name of source OS user
# - dbuser - DB user name
# - password - DB user password
# - dsource_name - name of dsource in environment
# - group - dsource  group
# - logsync
# - env - staging environment
# - inst - staging instance
# - stageuser - staging OS user
# - backup_dir - backup Location
# - dumppwd - backup Password
# - staging_mount - in 5.2 you can select it
# Start job to add Sybase dSource
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
    my $mountbase = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::addSource",1);

    my $config = $self->setConfig($source, $source_inst, $source_env);

    if (! defined($config)) {
        print "Source database $source not found\n";
        return undef;
    }

    if ($self->{_sourceconfig}->validateDBCredentials($config->{reference}, $dbuser, $password)) {
        print "Username or password is invalid.\n";
        return undef;
    }

    if ( $self->setGroup($group) ) {
        print "Group $group not found. dSource won't be created\n";
        return undef;
    }

    if ( $self->setEnvironment($env) ) {
        print "Staging environment $env not found. dSource won't be created\n";
        return undef;
    }

    if ( $self->setHome($inst) ) {
        print "Staging instance $inst in environment $env not found. dSource won't be created\n";
        return undef;
    }

    my $stagingrepo = $self->{"NEWDB"}->{"sourceConfig"}->{"repository"};

    if ($self->{_repository}->check_and_enable_staging($stagingrepo) ne 0) {
      print "Problem with enabling staging environment\n";
      return undef;
    }

    my $source_env_ref = $self->{_repository}->getEnvironment($config->{repository});

    my $source_os_ref = $self->{_environment}->getEnvironmentUserByName($source_env_ref,$source_osuser);

    if (!defined($source_os_ref)) {
        print "Source OS user $source_osuser not found\n";
        return undef;
    }

    my $stage_osuser_ref = $self->{_environment}->getEnvironmentUserByName($env,$stage_osuser);

    if (!defined($stage_osuser_ref)) {
        print "Source OS user $stage_osuser not found\n";
        return undef;
    }

    my $logsync_param = $logsync eq 'yes' ? JSON::true : JSON::false;

    my %dsource_params;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
      %dsource_params = (
          "type" => "ASELinkParameters",
          "container" => {
              "type" => "ASEDBContainer",
              "name" => $dsource_name,
              "group" => $self->{"NEWDB"}->{"container"}->{"group"},
              "sourcingPolicy" => {
                "logsyncEnabled" => $logsync_param,
                "type" => "SourcingPolicy"
              },
          },
          "sourceHostUser" => $source_os_ref,
          "stagingHostUser" => $stage_osuser_ref,
          "source" => {
              "type" => "ASELinkedSource",
              "config" => $config->{reference},
              "loadBackupPath" => $backup_dir
          },
          "dbCredentials" => {
              "type" => "PasswordCredential",
              "password" => $password
          },
          "dbUser" => $dbuser,
          "stagingRepository"=> $stagingrepo,
          "syncParameters"=> {
              "type"=> "ASELatestBackupSyncParameters"
          }
      );

      if (defined($dumppwd)) {
          $dsource_params{source}{dumpCredentials}{type} = "PasswordCredential";
          $dsource_params{source}{dumpCredentials}{password} = $dumppwd;
      }

    } else {
        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "ASELinkData",
              "config" => $config->{reference},
              "sourcingPolicy" => {
                  "type" => "SourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "dbCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
              },
              "dbUser" => $dbuser,
              "environmentUser" => $source_os_ref,
              "sourceHostUser" => $source_os_ref,
              "stagingHostUser" => $stage_osuser_ref,
              "stagingRepository"=> $stagingrepo,
              "loadBackupPath" => $backup_dir,
              "syncParameters"=> {
                  "type"=> "ASELatestBackupSyncParameters"
              }
          }
      );

      if (defined($dumppwd)) {
          $dsource_params{linkData}{dumpCredentials}{type} = "PasswordCredential";
          $dsource_params{linkData}{dumpCredentials}{password} = $dumppwd;
      }

    }



    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.9.0)) {
      if (defined($mountbase)) {
          $dsource_params{linkData}{mountBase} = $mountbase;
      }
    }

    my $ds_hooks = $self->set_dsource_hooks();
    if (defined($ds_hooks)) {
      if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
        $dsource_params{"source"}{"operations"} = $ds_hooks;
      } else {
        $dsource_params{"linkData"}{"operations"} = $ds_hooks;
      }
    }

    my $operation = 'resources/json/delphix/database/link';
    my $json_data =to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    logger($self->{_debug}, $json_data, 1);
    # there is couple of jobs - we need to monitor action
    return $self->runJobOperation($operation,$json_data, 'ACTION');

}

# Procedure setMountPoint
# parameters:
# - mountpoint - mount point
# Set mountpoint for new db.

sub setMountPoint {
    my $self = shift;
    my $mountpoint = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::setMountPoint",1);

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.9.0)) {
      if (defined($mountpoint)) {
        $self->{"NEWDB"}->{"source"}->{"mountBase"} = $mountpoint;
      }
    }


}




# Procedure attach_dsource
# parameters:
# - source - name of source DB
# - source_osuser - name of source OS user
# - dbuser - DB user name
# - password - DB user password
# - dsource_name - name of dsource in environment
# - group - dsource  group
# - env - staging environment
# - inst - staging instance
# - stageuser - staging OS user
# Start job to add Sybase dSource
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef

sub attach_dsource {
    my $self = shift;
    my $source = shift;
    my $source_inst = shift;
    my $source_env = shift;
    my $source_osuser = shift;
    my $dbuser = shift;
    my $password = shift;
    my $env = shift;
    my $inst = shift;
    my $stage_osuser = shift;
    my $backup_dir = shift;


    logger($self->{_debug}, "Entering SybaseVDB_obj::attachSource",1);

    my $config = $self->setConfig($source, $source_inst, $source_env);

    if (! defined($config)) {
        print "Source database $source not found\n";
        return undef;
    }

    if ($self->{_sourceconfig}->validateDBCredentials($config->{reference}, $dbuser, $password)) {
        print "Username or password is invalid.\n";
        return undef;
    }

    if ( $self->setEnvironment($env) ) {
        print "Staging environment $env not found. dSource won't be attached\n";
        return undef;
    }

    if ( $self->setHome($inst) ) {
        print "Staging instance $inst in environment $env not found. dSource won't be attached\n";
        return undef;
    }

    my $stagingrepo = $self->{"NEWDB"}->{"sourceConfig"}->{"repository"};

    my $source_env_ref = $self->{_repository}->getEnvironment($config->{repository});

    my $source_os_ref = $self->{_environment}->getEnvironmentUserByName($source_env_ref,$source_osuser);

    if (!defined($source_os_ref)) {
        print "Source OS user $source_osuser not found\n";
        return undef;
    }

    my $stage_osuser_ref = $self->{_environment}->getEnvironmentUserByName($env,$stage_osuser);

    if (!defined($stage_osuser_ref)) {
        print "Source OS user $stage_osuser not found\n";
        return undef;
    }

    my %dsource_params;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
      %dsource_params = (
          "type" => "ASEAttachSourceParameters",
          "sourceHostUser" => $source_os_ref,
          "stagingHostUser" => $stage_osuser_ref,
          "source" => {
              "type" => "ASELinkedSource",
              "config" => $config->{reference},
              "loadBackupPath" => $backup_dir
          },
          "dbCredentials" => {
              "type" => "PasswordCredential",
              "password" => $password
          },
          "dbUser" => $dbuser,
          "stagingRepository"=> $stagingrepo
      );
    } else {
      %dsource_params = (
          "type" => "AttachSourceParameters",
          "attachData" => {
                "type" => "ASEAttachData",
                "config" => $config->{reference},
                "dbCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
                },
                "dbUser" => $dbuser,
                "environmentUser" => $source_os_ref,
                "loadBackupPath" => $backup_dir,
                "stagingRepository"=> $stagingrepo,
                "sourceHostUser" => $source_os_ref,
                "stagingHostUser" => $stage_osuser_ref
          }
      );
    }






    my $operation = 'resources/json/delphix/database/'. $self->{container}->{reference} .'/attachSource' ;
    my $json_data =to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    logger($self->{_debug}, $json_data, 1);
    # there is couple of jobs - we need to monitor action
    return $self->runJobOperation($operation,$json_data, 'ACTION');

}



# Procedure createVDB
# parameters:
# - group - new DB group
# - env - new DB environment
# - inst - new DB instance
# Start job to create Sybase VBD
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef

sub createVDB {
    my $self = shift;

    my $group = shift;
    my $env = shift;
    my $inst = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::createVDB",1);


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

    if ( $self->setHost() ) {
        print "Host is not set. VDB won't be created\n";
        return undef;
    }

    if ( ! defined($self->{"NEWDB"}->{"container"}->{"name"} ) ) {
        print "Set name using setName procedure before calling create VDB. VDB won't be created\n";
        return undef;
    }

    delete $self->{"NEWDB"}->{"sourceConfig"}->{"linkingEnabled"};


    my $operation = 'resources/json/delphix/database/provision';
    my $json_data = $self->getJSON();
    return $self->runJobOperation($operation,$json_data);

}


# Procedure v2p
# parameters:
# - env - new DB environment
# - inst - new DB instance
# Start job for v2p Sybase VBD
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef

sub v2p {
    my $self = shift;

    my $env = shift;
    my $inst = shift;

    logger($self->{_debug}, "Entering SybaseVDB_obj::v2p",1);


    if ( $self->setHome($inst) ) {
        print "Instance $inst in environment $env not found. VDB won't be created\n";
        return undef;
    }

    if ( $self->setHost() ) {
        print "Host is not set. VDB won't be created\n";
        return undef;
    }

    if ( ! defined($self->{"NEWDB"}->{"container"}->{"name"} ) ) {
        print "Set name using setName procedure before calling create VDB. VDB won't be created\n";
        return undef;
    }

    $self->{"NEWDB"}->{"type"} = "ASEExportParameters";


    delete $self->{"NEWDB"}->{"truncateLogOnCheckpoint"};
    delete $self->{"NEWDB"}->{"container"};
    delete $self->{"NEWDB"}->{"source"};
    my $operation = 'resources/json/delphix/database/export';
    my $json_data = $self->getJSON();
    return $self->runJobOperation($operation,$json_data);

}



# Procedure setLogTruncate
# parameters:
# - logtruncate option - true / false
# Set log truncate option

sub setLogTruncate {
    my $self = shift;
    my $logtrunc = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::setLogTruncate",1);

    if (defined($logtrunc)) {
        $self->{"NEWDB"}->{"truncateLogOnCheckpoint"} = JSON::true;
    }

    #print Dumper $self->{"NEWDB"};
}


# Procedure getLogTruncate
# parameters:
# Return value of log truncate option

sub getLogTruncate {
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::getLogTruncate",1);
    my $ret;

    if ($self->{"source"}->{"runtime"}->{"status"} eq 'RUNNING') {
      $ret = $self->{"source"}->{"runtime"}->{"truncateLogOnCheckpoint"} ? "enabled" : "disabled";
    } else {
      $ret = 'N/A';
    }

    return $ret;

    #print Dumper $self->{"NEWDB"};
}


# Procedure setName
# parameters:
# - contname - container name
# - dbname - database name
# Set name for new db.

sub setName {
    my $self = shift;
    my $contname = shift;
    my $dbname = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::setName",1);

    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"databaseName"} = $dbname;

}

# Procedure setHost
# parameters:
# Set host reference for new db. Host reference is set by setEnvironment method
# Return 0 if success, 1 if not found

sub setHost {
    my $self = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::setHost",1);

    if (defined ($self->{'_hosts'})) {
        $self->{"NEWDB"}->{"sourceConfig"}->{"instance"}->{"host"} = $self->{'_hosts'};
        return 0;
    } else {
        return 1;
    }

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
    logger($self->{_debug}, "Entering SybaseVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};


    #my $sources = new Source_obj($dlpxObject, $debug);

    #print Dumper $name;

    #my $sourceitem = $sources->getSourceByName($name);

    if (defined ($sourceitem)) {
#        my $sourcetype = $sourceitem->{'type'};

        my $sourcetype = $sourceitem->{container}->{'type'};

        if (($sourcetype eq 'ASEDBContainer') || ($sourcetype eq 'ASEVirtualSource')) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}  = $sourceitem->{container}->{reference};
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }

}

# Procedure snapshot
# parameters:
# - frombackup - yes/no
# - list of files
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot
{
    my $self = shift;
    my $frombackup = shift;
    my $files = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::snapshot",1);

    my %snapshot_type;

    if (scalar(@{$files}) > 0) {
      %snapshot_type = (
          "type" => "ASESpecificBackupSyncParameters",
          "backupFiles" => $files
      );
    } elsif (! defined ($frombackup) ) {
        return undef;
    } else {
      if ( $frombackup eq "yes" ) {
          %snapshot_type = (
              "type" => "ASELatestBackupSyncParameters"
          );
      } else {
          %snapshot_type = (
              "type" => "ASENewBackupSyncParameters"
          );
      }
    }


    if ($self->getType() eq 'VDB') {
        %snapshot_type = (
            "type" => "ASELatestBackupSyncParameters"
        );
    }

    return $self->VDB_obj::snapshot(\%snapshot_type) ;
}



# Procedure setEncryption
# parameters:
# - password - encryption key
# set backup password
# Return job number if provisioning has been started, otherwise return undef

sub setEncryption {
    my $self = shift;
    my $password = shift;
    my $ret;


    logger($self->{_debug}, "Entering SybaseVDB_obj::setEncryption",1);

    my $source = $self->{source}->{reference};

    my %encryption_hash = (
        type => "ASELinkedSource",
        dumpCredentials=> {
            password=> $password,
            type=> "PasswordCredential"
        }
    );

    if ($password eq '') {
      $encryption_hash{dumpCredentials} = JSON::null;
    }


    my $json_data = encode_json(\%encryption_hash);

    my $operation = 'resources/json/delphix/source/' . $source;

    logger($self->{_debug}, $json_data, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if (! defined ($result) ) {
        print "There was a problem with setting a key for database " . $self->getName() . ". \n";
        $ret = 1;
    } elsif ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        print "Encryption key for database " . $self->getName() . " set with success.\n";
        $ret = 0;
    } else {
        print "There was a problem with setting a key for database " . $self->getName() . ". \n";
        $ret = 1;
    }


    return $ret;

}

# Procedure setCredentials
# parameters:
# - username
# - password
# - force - skip check password if defined (doesn't work for Oracle - check is a part of API)
# Set credentials for a db
# Return 0 if success, 1 if not found

sub setCredentials {
    my $self = shift;
    my $username = shift;
    my $password = shift;
    my $force = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::setCredentials",1);

    my $ret;

    if ($self->{_sourceconfig}->setCredentials($self->{sourceConfig}->{reference}, $username, $password, $force)) {
        print "Username or password is invalid.\n";
        $ret = 1;
    } else {
        $ret = 0;
    }


    if ($ret eq 0) {
      my $jobid = $self->{_environment}->changeASEPassword($self->{environment}->{reference}, $password);
      $ret = $ret + Toolkit_helpers::waitForAction($self->{_dlpxObject}, $jobid, "Password changed on environment", "Problem with password change");
    }


    return $ret;


}

# Procedure update_dsource
# parameters:
# - backup_dir (implemented for ms sql and sybase)
# - logsync (implemented for oracle)
# - validatedsync  (implemented for ms sql and sybase)

sub update_dsource {
    my $self = shift;
    my $backup_dir = shift;
    my $logsync = shift;
    my $validatedsync = shift;


    logger($self->{_debug}, "Entering SybaseVDB_obj::update_dsource",1);

    my %source_hash;
    my $jobno;

    my $update = 0;
    my $dbtype = $self->getDBType();

    %source_hash = (
        "type" => $self->{source}->{type}
    );



    if (defined($backup_dir)) {
      $self->setBackupPath(\%source_hash, $backup_dir);
      $update = 1;
    }

    if (defined($validatedsync)) {
      if ($self->setValidatedMode(\%source_hash, $validatedsync)) {
        return undef;
      }
      $update = 1;
    }


    if ($update eq 1) {
      my $json_data = to_json(\%source_hash);

      logger($self->{_debug}, $json_data ,2);

      my $operation = 'resources/json/delphix/source/' . $self->{source}->{reference};
      $jobno = $self->runJobOperation($operation, $json_data, 'ACTION');
    } else {
      print "Nothing to update for 1st part\n";
    }

    return $jobno;

}

1;
