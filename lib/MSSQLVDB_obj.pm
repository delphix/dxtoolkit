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
# Program Name : OracleVDB_obj.pm
# Description  : Delphix Engine Database objects
# It's include the following classes:
# - MSSQLVDB_obj - MS SQL VDB
#
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
# class MSSQLVDB_obj - is a child class of VDB_obj

package MSSQLVDB_obj;
use Data::Dumper;
use strict;
use warnings;
use JSON;    
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;    
    logger($debug, "Entering MSSQLVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug); 



    # MS SQL specific properties 
    my %prov = (
            "type" => "MSSqlProvisionParameters",
            "recoveryModel" => "SIMPLE",
            "container" => {
                "type" => 'MSSqlDatabaseContainer',
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
                    "type" => "MSSqlSIConfig",
                    "linkingEnabled" => JSON::false,
                    "repository" => "",
                    "databaseName" => "",
                    "instance" => {
                        "type" => "MSSqlInstanceConfig"
                    }
            },
            "source" => {
                    "type" => "MSSqlVirtualSource"
            },
            "timeflowPointParameters" => {
                "type" => "TimeflowPointSemantic",
                "container" => "",
                "location" => "LATEST_SNAPSHOT"
            },
    );

    $self->{"NEWDB"} = \%prov;
    $self->{_dbtype} = 'mssql';
    return $self;
}


# Procedure getConfig
# parameters: none
# Return database config

sub getConfig 
{
    my $self = shift;
    my $templates = shift;
    my $backup = shift;

    logger($self->{_debug}, "Entering MSSQLVDB_obj::getConfig",1);
    my $config = '';
    my $joinsep;
    
    if (defined($backup)) {
      $joinsep = ' ';
    } else {
      $joinsep = ',';
    }
    
    if ($self->getType() eq 'VDB') {
      my $recoveryModel = $self->getRecoveryModel();       
      $config = join($joinsep,($config, "-recoveryModel $recoveryModel"));
    } elsif ($self->getType() eq 'dSource')  {
      my $staging_user = $self->getStagingUser();
      my $staging_env = $self->getStagingEnvironment();
      my $staging_inst = $self->getStagingInst();
                
      $config = join($joinsep,($config, "-stageinst \"$staging_inst\""));
      $config = join($joinsep,($config, "-stageenv \"$staging_env\""));
      $config = join($joinsep,($config, "-stage_os_user \"$staging_user\""));
    
      my $backup_path = $self->getBackupPath();
      if (!defined($backup_path)) {
        #autobackup path
        $backup_path = "";
      }
      if (defined($backup_path)) {
        $backup_path =~ s/\\/\\\\/g;
      }
      my $vsm = $self->getValidatedMode();
      my $dmb = $self->getDelphixManaged();

      if ($dmb eq 'yes') {
        $config = join($joinsep,($config, "-delphixmanaged $dmb"));
      } else {
        $config = join($joinsep,($config, "-validatedsync $vsm -backup_dir \"$backup_path\""));
      }
      
    } else {
      $config = '';
    }

    if ( (my $rest) = $config =~ /^,(.*)/ ) {
      $config = $rest;
    }

    return $config;
    
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
    logger($self->{_debug}, "Entering MSSQLVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};
    

    #my $sources = new Source_obj($dlpxObject, $debug);
    #my $sourceitem = $sources->getSourceByName($name);


    if (defined ($sourceitem)) {
        #my $sourcetype = $sourceitem->{'type'};
        my $sourcetype = $sourceitem->{container}->{'type'};
        if (($sourcetype eq 'MSSqlDatabaseContainer') || ($sourcetype eq 'MSSqlVirtualSource')) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}  = $sourceitem->{container}->{reference};
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }       

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
    logger($self->{_debug}, "Entering MSSQLVDB_obj::setName",1);
    
    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"databaseName"} = $dbname;
    
}

# Procedure setHost
# parameters: 
# Set host reference for new db. Host reference is set by setEnvironment method
# Return 0 if success, 1 if not found

sub setHost {
    my $self = shift; 
    logger($self->{_debug}, "Entering MSSQLVDB_obj::setHost",1);

    if (defined ($self->{'_hosts'})) {
        $self->{"NEWDB"}->{"sourceConfig"}->{"instance"}->{"host"} = $self->{'_hosts'};
        return 0;
    } else {
        return 1;
    }     

}


# Procedure snapshot
# parameters: 
# - frombackup - yes/no
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot 
{
    my $self = shift;
    my $frombackup = shift;
    logger($self->{_debug}, "Entering MSSQLVDB_obj::snapshot",1);

    if (! defined ($frombackup) ) {
        return undef;
    };

    my $frombackup_json;

    if ( $frombackup eq "yes" ) {
        $frombackup_json = JSON::true;
    } else {
        $frombackup_json = JSON::false;
    }

    my %snapshot_type;

    if ($self->getType() eq 'VDB') {
        %snapshot_type = (
            "type" => "MSSqlSyncParameters"
        );
    }
    else {
        %snapshot_type = (
            "type" => "MSSqlSyncParameters",
            "loadFromBackup" => $frombackup_json
        );
    }
    return $self->VDB_obj::snapshot(\%snapshot_type) ;
}

# Procedure setFileSystemLayout
# parameters: 
# - map_file - hash of map file
# Set mountpoint for new db. 

sub setFileSystemLayout {
    my $self = shift; 
    my $targetDirectory = shift;
    my $archiveDirectory = shift;
    my $dataDirectory = shift;
    my $externalDirectory = shift;
    my $scriptDirectory = shift;
    my $tempDirectory = shift;

    logger($self->{_debug}, "Entering MSSQLVDB_obj::setFileSystemLayout",1);

    $self->{"NEWDB"}->{"filesystemLayout"}->{"type"} = "TimeflowFilesystemLayout";

    if (! defined($targetDirectory)) {
        return 1;
    }

    $self->{"NEWDB"}->{"filesystemLayout"}->{"targetDirectory"} = $targetDirectory;

    if (defined($dataDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"dataDirectory"} = $dataDirectory;
    } else {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"dataDirectory"} = "data";    
    }


    if ( defined($archiveDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"archiveDirectory"} = $archiveDirectory;
    } else {
         $self->{"NEWDB"}->{"filesystemLayout"}->{"archiveDirectory"} = "logs";       
    }

    if ( defined($tempDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"tempDirectory"} = $tempDirectory;
    }

    if ( defined($scriptDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"scriptDirectory"} = $scriptDirectory;
    } else {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"scriptDirectory"} = "scripts";     
    }

    if ( defined($externalDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"externalDirectory"} = $externalDirectory;
    }

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
    my $validatedSyncMode = shift;
    my $delphixmanaged = shift;

    logger($self->{_debug}, "Entering MSSQLVDB_obj::addSource",1);
    
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

    # if ( $self->setHost() ) {
    #     print "Host is not set. VDB won't be created\n";
    #     return undef;
    # }

    # if ( ! defined($self->{"NEWDB"}->{"container"}->{"name"} ) ) {
    #     print "Set name using setName procedure before calling create VDB. VDB won't be created\n";
    #     return undef;
    # }


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
    
    my $vsm;
    
    if (!defined($validatedSyncMode)) {
      $vsm = "NONE";
    } else {
      if ( (uc $validatedSyncMode eq 'NONE' ) || (uc $validatedSyncMode eq 'FULL_OR_DIFFERENTIAL' ) || (uc $validatedSyncMode eq 'FULL' ) || (uc $validatedSyncMode eq 'TRANSACTION_LOG' ) ) 
      {
        $vsm = $validatedSyncMode;
      } else {
        print "Invalid validatedSyncMode option - $validatedSyncMode \n";
        return undef;
      }
                                  
    }



    my %dsource_params;
    
    
    if ($self->{_dlpxObject}->getApi() lt "1.8") {


      if (defined($delphixmanaged) && ($delphixmanaged eq 'yes')) {
          %dsource_params = (
              "type" => "MSSqlLinkParameters",
              "container" => {
                  "type" => "MSSqlDatabaseContainer",
                  "name" => $dsource_name,
                  "group" => $self->{"NEWDB"}->{"container"}->{"group"},
                  "delphixManaged" => JSON::true
              },
              "source" => {
                  "type" => "MSSqlLinkedSource",
                  "config" => $config->{reference}
              },
              "dbCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
              },
              "dbUser" => $dbuser,
              "pptRepository" => $stagingrepo
          );
      } else {
        
        #print Dumper $backup_dir;
              
        %dsource_params = (
            "type" => "MSSqlLinkParameters",
            "container" => {
                "type" => "MSSqlDatabaseContainer",
                "name" => $dsource_name,
                "group" => $self->{"NEWDB"}->{"container"}->{"group"},
                "sourcingPolicy" => {
                  "logsyncEnabled" => $logsync_param,
                  "loadFromBackup" => JSON::true,
                  "type" => "SourcingPolicy"
                },
            },
            "sourceHostUser" => $source_os_ref,
            "pptHostUser" => $stage_osuser_ref,
            "source" => {
                "type" => "MSSqlLinkedSource",
                "config" => $config->{reference},
                "sharedBackupLocation" => $backup_dir,
                "validatedSyncMode" => $vsm,
                "operations" => {
                  "type" => "LinkedSourceOperations",
                  "preSync" => [],
                  "postSync" => []
                },
            },
            "dbCredentials" => {
                "type" => "PasswordCredential",
                "password" => $password
            },
            "dbUser" => $dbuser,
            "pptRepository"=> $stagingrepo
        );
        
        if ($backup_dir eq '') {
          # autobackup dir set
          delete $dsource_params{source}{sharedBackupLocation};
        }
        
      }

      if (defined($dumppwd)) {
        $dsource_params{source}{encryptionKey} = $dumppwd;
      }

      
    } else {
        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "MSSqlLinkData",
              "config" => $config->{reference},
              "sourcingPolicy" => {
                  "type" => "SourcingPolicy",
                  "logsyncEnabled" => $logsync_param,
                  "loadFromBackup" => JSON::true
              },
              "dbCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
              },
              "dbUser" => $dbuser,
              "environmentUser" => $source_os_ref,
              "sharedBackupLocation" => $backup_dir,
              "validatedSyncMode" => $vsm,
              "sourceHostUser" => $source_os_ref,
              "pptHostUser" => $stage_osuser_ref,
              "pptRepository"=> $stagingrepo
            }
          );
          
          if (defined($delphixmanaged) && ($delphixmanaged eq 'yes')) {
            $dsource_params{"linkData"}{"delphixManaged"} = JSON::true;
            delete $dsource_params{"linkData"}{"sourcingPolicy"}{"loadFromBackup"};
          }
    }    
    

    my $operation = 'resources/json/delphix/database/link';
    my $json_data =to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    logger($self->{_debug}, $json_data, 1);
    # there is couple of jobs - we need to monitor action
    return $self->runJobOperation($operation,$json_data, 'ACTION');

}



# Procedure createVDB
# parameters: 
# - source - dsource name
# - group - new DB group
# - env - new DB environment
# - inst - new DB instance
# Start job to create MS SQL VBD (by default is using recoveryModel - SIMPLE)
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef 

sub createVDB {
    my $self = shift; 

    my $group = shift;
    my $env = shift;
    my $inst = shift;

    logger($self->{_debug}, "Entering MSSQLVDB_obj::createVDB",1);


    if ( $self->setGroup($group) ) {
        print "Group $group not found. VDB won't be created\n";
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

    if ($self->{_dlpxObject}->getApi() ge "1.8") {
      $self->{"NEWDB"}->{"source"}->{"allowAutoVDBRestartOnHostReboot"} = JSON::false;
    }
    


    my $operation = 'resources/json/delphix/database/provision';
    my $json_data = $self->getJSON();
    return $self->runJobOperation($operation,$json_data);

}


# Procedure v2pSI
# parameters: 
# - env - new DB environment
# - home - new DB home
# Start job to create Single Instance Oracle V2P
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef 

sub v2p {
    my $self = shift; 

    my $env = shift;
    my $home = shift;


    logger($self->{_debug}, "Entering MSSQLVDB_obj::v2p",1);

    if ( $self->setEnvironment($env) ) {
        print "Environment $env not found. V2P won't be created\n";
        return undef;
    }

    if ( $self->setHome($home) ) {
        print "Home $home in environment $env not found. V2P won't be created\n";
        return undef;
    }

    if ( ! defined($self->{"NEWDB"}->{"container"}->{"name"} ) ) {
        print "Set name using setName procedure before calling create V2P. V2P won't be created\n";
        return undef;
    }

    if ( ! defined($self->{"NEWDB"}->{"filesystemLayout"}->{"type"} )) {
        print "Target directory not set. V2P won't be created\n";
        return undef;
    }

    if ( $self->setHost() ) {
        print "Host is not set. V2P won't be created\n";
        return undef;
    }

    $self->{"NEWDB"}->{"type"} = "MSSqlExportParameters";
    $self->{"NEWDB"}->{"recoverDatabase"} = JSON::true;
    

    # this two sections are not used in V2P
    delete $self->{"NEWDB"}->{"container"};
    delete $self->{"NEWDB"}->{"source"};
    delete $self->{"NEWDB"}->{"sourceConfig"}->{"linkingEnabled"};

    #run job
    my $operation = 'resources/json/delphix/database/export';
    my $json_data = $self->getJSON();
    return $self->runJobOperation($operation,$json_data);

}

# Procedure preScript
# parameters: 
# - script - path
# Add pre script for provisioning

sub setPreScript {
    my $self = shift; 
    my $path = shift;
    logger($self->{_debug}, "Entering MSSQLVDB_obj::setPreScript",1);
    $self->{"NEWDB"}->{"source"}->{"preScript"} = $path;

}


# Procedure postScript
# parameters: 
# - script - path
# Add pre script for provisioning

sub setPostScript {
    my $self = shift; 
    my $path = shift;
    logger($self->{_debug}, "Entering MSSQLVDB_obj::setPostScript",1);
    $self->{"NEWDB"}->{"source"}->{"postScript"} = $path;

}

# Procedure upgradeVDB
# parameters: 
# - home - new DB home
# Upgrade VDB
# Return job number if provisioning has been started, otherwise return undef 

sub upgradeVDB {
    my $self = shift; 
    my $home = shift;
    my $ret;


    logger($self->{_debug}, "Entering MSSQLVDB_obj::upgradeVDB",1);
    return $self->VDB_obj::upgradeVDB($home,'MSSqlSIConfig') ;

}

# Procedure getBackupPath
# parameters: 
# Return backup path

sub getBackupPath {
    my $self = shift; 

    logger($self->{_debug}, "Entering MSSQLVDB_obj::getBackupPath",1);
    return $self->{source}->{sharedBackupLocation};

}

# Procedure setRecoveryModel
# parameters: 
# Return recovery mode

sub setRecoveryModel {
    my $self = shift; 
    my $recoveryModel = shift;

    logger($self->{_debug}, "Entering MSSQLVDB_obj::setRecoveryModel",1);
    if ( ( uc $recoveryModel eq 'BULK_LOGGED' ) || ( uc $recoveryModel eq 'FULL' ) || ( uc $recoveryModel eq 'SIMPLE' ) ) {
      $self->{"NEWDB"}->{"sourceConfig"}->{recoveryModel} = $recoveryModel;
      return 0;
    } else {
      return 1;
    }
}

# Procedure getRecoveryMode
# parameters: 
# Return recovery mode

sub getRecoveryModel {
    my $self = shift; 

    logger($self->{_debug}, "Entering MSSQLVDB_obj::getRecoveryModel",1);
    return $self->{sourceConfig}->{recoveryModel};

}

# Procedure getValidatedMode
# parameters: 
# Return validated mode

sub getValidatedMode {
    my $self = shift; 

    logger($self->{_debug}, "Entering MSSQLVDB_obj::getValidatedMode",1);
    my $ret;
    if (defined($self->{source}->{validatedSyncMode})) {
      $ret = $self->{source}->{validatedSyncMode};
    } else {
      $ret = 'N/A';
    }
    return $ret;

}

# Procedure getDelphixManaged
# parameters: 
# Return validated mode

sub getDelphixManaged {
    my $self = shift; 

    logger($self->{_debug}, "Entering MSSQLVDB_obj::getDelphixManaged",1);
    my $ret;
    if (defined($self->{container}->{delphixManaged})) {
      $ret = $self->{container}->{delphixManaged} ? 'yes' : 'no';
    }
    return $ret;

}

# Procedure attach_dsource
# parameters: 
# - dbuser 
# - dbpassword 
# - envuser 
# - envsrc
# - srcdb 
# attach dsource
# Return job number if job started or undef otherwise

sub attach_dsource 
{
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
    my $vsm = shift;

    logger($self->{_debug}, "Entering MSSQLVDB_obj::attachSource",1);

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

    my @preSync;
    my @postSync;

    my %operations = (
        "type" => "LinkedSourceOperations",
        "preSync" => \@preSync,
        "postSync" => \@postSync
    );
    
    
    my %attach_data;
    if ($self->{_dlpxObject}->getApi() lt "1.8") {
      %attach_data = (
          "type" => "MSSqlAttachSourceParameters",
          "source" =>  {
              "type" => "MSSqlLinkedSource",
              "config" => $config->{reference},
              "operations" => \%operations,
              "sharedBackupLocation" => $backup_dir,
              "validatedSyncMode" => $vsm
          },
          "dbCredentials" => {
              "type" => "PasswordCredential",
              "password" => $password
          },
          "dbUser" => $dbuser,
          "pptRepository" => $stagingrepo,
          "sourceHostUser" => $source_os_ref,
          "pptHostUser" => $stage_osuser_ref
      );
    } else {
      %attach_data = (
          "type" => "AttachSourceParameters",
          "attachData" =>  {
              "type" => "MSSqlAttachData",
              "config" => $config->{reference},
              "operations" => \%operations,
              "sharedBackupLocation" => $backup_dir,
              "dbCredentials" => {
                "type" => "PasswordCredential",
                "password" => $password
              },
              "dbUser" => $dbuser,
              "pptRepository" => $stagingrepo,
              "sourceHostUser" => $source_os_ref,
              "pptHostUser" => $stage_osuser_ref,
              "validatedSyncMode" => $vsm
          }
      );
    }

    my $operation = "resources/json/delphix/database/" . $self->{container}->{reference} . "/attachSource";
    my $json_data = encode_json(\%attach_data);

    #print Dumper $json_data;

    return $self->runJobOperation($operation,$json_data, 'ACTION');    
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


    logger($self->{_debug}, "Entering MSSQLVDB_obj::setEncryption",1);

    my $source = $self->{source}->{reference};

    my %encryption_hash = (
        type => "MSSqlLinkedSource",  
        encryptionKey=>$password
    );

    my $json_data = encode_json(\%encryption_hash);

    logger($self->{_debug}, $json_data, 2);

    my $operation = 'resources/json/delphix/source/' . $source;

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


1;
