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
# Program Name : VDB_obj.pm
# Description  : Delphix Engine Database objects
# It's include the following classes:
# - VDB_obj - generic class of database object
# - OracleVDB_obj - Oracle VDB
# - MSSQLVDB_obj - MS SQL VDB
#
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#




#######################

package VDB_obj;

use JSON;
use strict;
use warnings;
use Data::Dumper;
use Date::Manip;
use version;

use Group_obj;
use Host_obj;
use Source_obj;
use Snapshot_obj;
use Action_obj;
require Namespace_obj;
use Bookmark_obj;
use SourceConfig_obj;
use Environment_obj;
use Repository_obj;
use Toolkit_helpers qw (logger);
use Op_template_obj;

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)


sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering VDB_obj::constructor",1);

    # define object properties
    # NEWDB - is a set of settings to provision a new VDB based on defaults
    my $self = {
        _dlpxObject => $dlpxObject,
        _debug => $debug,
        _dbtype => 'GENERIC'
   };



    bless($self,$classname);
    return $self;
}

# Procedure getJSON
# parameters: none
# Return JSON encoded parameters to provision a new VDB

sub getJSON
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getJSON",1);
    my $json_data = encode_json($self->{"NEWDB"});
    my $result_fmt = to_json($self->{"NEWDB"}, {pretty=>1});
    logger($self->{_debug},$result_fmt,2);
    return $json_data;
}

# Procedure getName
# parameters: none
# Return database name

sub getName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getName",1);

    my $ret;

    if ($self->isReplica() eq 'YES') {
        my $namespace_name = $self->{"namespace"}->getName($self->getNamespace());
        $ret = $self->{container}->{name} . "@" . $namespace_name;
    } else {
        $ret = $self->{container}->{name};
    }

    return $ret;
}

# Procedure getDBType
# parameters: none
# Return database type

sub getDBType
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getDBType",1);
    return $self->{_dbtype};
}

# Procedure isReplica
# parameters: none
# Return is this db is a replica or not

sub isReplica
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::isReplica",1);
    return $self->{container}->{namespace} ? 'YES' : 'NO';
}

# Procedure getNamespace
# parameters: none
# Return database namespace

sub getNamespace
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getNamespace",1);
    return $self->{container}->{namespace};
}

# Procedure getCreationTime
# parameters: none
# Return database creation time

sub getCreationTime
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getCreationTime",1);
    return $self->{container}->{creationTime};
}

# Procedure getMasked
# parameters: none
# Return masked or non-masked status

sub getMasked
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getMasked",1);
    return $self->{container}->{masked};
}

# Procedure getMaskingJob
# parameters: none
# Return masked job

sub getMaskingJob
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getMaskingJob",1);
    my $ret = '';

    for my $hook (@{$self->{source}->{operations}->{configureClone}}) {
      if ($hook->{type} eq 'RunMaskingJobOnSourceOperation') {
        $ret = $hook->{name};
      }
    }

    return $ret;
}


# Procedure setMaskingJob
# parameters:
# - maskingjob - reference of masking job


sub setMaskingJob
{
    my $self = shift;
    my $maskingjob = shift;
    logger($self->{_debug}, "Entering VDB_obj::setMaskingJob",1);

    if ($maskingjob eq 'script') {
      if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.10.1)) {
        $self->{"NEWDB"}->{masked} = JSON::true;
      } else {
        print "Script masking allowed from higher version of Delphix";
        return 1;
      }
    } else {
      $self->{"NEWDB"}->{maskingJob} = $maskingjob;
    }

    return 0;

}

# Procedure setNoRecovery
# parameters:



sub setNoRecovery
{
    my $self = shift;
    my $maskingjob = shift;
    logger($self->{_debug}, "Entering VDB_obj::setNoRecovery",1);

    $self->{"NEWDB"}->{recoverDatabase} = JSON::false;

}


# Procedure setAutostart
# parameters:
# - autostart - set yes to autostart
# set autostart during provisioning


sub setAutostart
{
    my $self = shift;
    my $autostart = shift;
    logger($self->{_debug}, "Entering VDB_obj::setAutostart",1);

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.8.0)) {
      if (defined($autostart) && (lc $autostart eq 'yes')) {
        $self->{"NEWDB"}->{"source"}->{"allowAutoVDBRestartOnHostReboot"} = JSON::true;
      } else {
        $self->{"NEWDB"}->{"source"}->{"allowAutoVDBRestartOnHostReboot"} = JSON::false;
      }
    }

}

# Procedure getAutostart
# parameters:
# get autostart


sub getAutostart
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getAutostart",1);

    my $ret;

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.8.0)) {
      $ret = $self->{source}->{allowAutoVDBRestartOnHostReboot} ? 'yes' : 'no';
    } else {
      $ret = 'N/A';
    }

    return $ret;

}



# Procedure changeAutostart
# parameters:
# - autostart - set yes to autostart
# set autostart of existing database


sub changeAutostart
{
    my $self = shift;
    my $autostart = shift;
    logger($self->{_debug}, "Entering VDB_obj::changeAutostart",1);

    my %source_hash;
    my $ret;

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.8.0)) {
      if (defined($autostart) && (lc $autostart eq 'yes')) {
        %source_hash = (
            "type" => $self->{source}->{type},
            "allowAutoVDBRestartOnHostReboot" => JSON::true
        );
      } else {
        %source_hash = (
            "type" => $self->{source}->{type},
            "allowAutoVDBRestartOnHostReboot" => JSON::false
        );
      }
    } else {
      return 1;
    }

    my $json_data = to_json(\%source_hash);

    logger($self->{_debug}, $json_data ,2);

    my $operation = 'resources/json/delphix/source/' . $self->{source}->{reference};
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      $ret = 0;
    } else {
      $ret = 1;
    }

    return $ret;
}


# Procedure getDbUser
# parameters: none
# Return database user

sub getDbUser
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getDbUser",1);
    my $ret;

    if ($self->{sourceConfig} ne 'NA') {
      if (defined($self->{sourceConfig}->{user})) {
        $ret = $self->{sourceConfig}->{user};
      } else {
        $ret = 'N/A';
      }
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getOSUser
# parameters: none
# Return OS user

sub getOSUser
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getOSUser",1);
    my $ret;
    my $user;
    if ($self->{sourceConfig} ne 'NA') {
      $user = $self->{sourceConfig}->{environmentUser};
      my $envref = $self->{"environment"}->{reference};
      if (defined($envref)) {
        $ret = $self->{_environment}->getEnvironmentUserNamebyRef($envref, $user);
      } else {
        $ret = 'N/A';
      }
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure getStagingUser
# parameters: none
# Return OS user

sub getStagingUser
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getStagingUser",1);
    my $ret;
    my $user;

    my $staging_env = $self->{staging_environment}->{reference};
    my $staging_user_ref;

    if (defined($staging_env)) {
      $staging_user_ref = $self->{staging_sourceConfig}->{environmentUser};
      $ret = $self->{_environment}->getEnvironmentUserNamebyRef($staging_env, $staging_user_ref);
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure getParentName
# parameters: none
# Return parent database if defined, otherwise empty string

sub getParentName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getParentName",1);

    return defined($self->{provisionContainer_name}) ? $self->{provisionContainer_name} : '';
}

# Procedure getParentContainer
# parameters: none
# Return parent database if defined, otherwise empty string

sub getParentContainer
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getParentContainer",1);
    return defined($self->{"container"}->{provisionContainer}) ? $self->{"container"}->{provisionContainer} : '';
}


# Procedure getTimezone
# parameters: none
# Return database timezone

sub getTimezone
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getTimezone",1);
    my $tz = $self->{host}->{hostConfiguration}->{operatingSystem}->{timezone};

    my @tztmp = split(',', $tz);
    return $tztmp[0];
}


# Procedure getReference
# parameters: none
# Return parent database if defined

sub getReference
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getReference",1);
    return $self->{container}->{reference};
}

# Procedure getLogSync
# parameters: none
# Return status of Log Sync

sub getLogSync
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getLogSync",1);
    return $self->{container}->{runtime}->{logSyncActive} ? 'ACTIVE' : 'INACTIVE';
}

# Procedure getGroup
# parameters: none
# Return parent database if defined

sub getGroup
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getGroup",1);
    return $self->{container}->{group};
}


# # Procedure getVersion
# # parameters: none
# # Return database version

# sub getVersion
# {
#     my $self = shift;
#     logger($self->{_debug}, "Entering VDB_obj::getVersion",1);
#     return $self->{repository}->{version};
# }

# Procedure getEnvironmentUserName
# parameters: none
# Return database environment

sub getEnvironmentUserName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getEnvironmentUserName",1);

    my $ret;

    if (defined($self->{_environment})) {
        $ret = $self->{_environment}->getEnvironmentUserNamebyRef($self->{environment}->{reference}, $self->{sourceConfig}->{environmentUser});
    } else {
        $ret = 'NA';
    }

    return $ret;
}




# Procedure getEnvironmentName
# parameters: none
# Return database environment

sub getEnvironmentName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getEnvironmentName",1);

    my $ret = 'NA';

    if (defined($self->{environment})) {
        $ret = $self->{environment}->{name};
    } else {
        $ret = 'NA';
    }

    return $ret;
}


# Procedure getDatabaseName
# parameters: none
# Return database name

sub getDatabaseName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getDatabaseName",1);
    if (defined($self->{sourceConfig})) {
      if ($self->{sourceConfig} eq "NA") {
        return "NA";
      } else {
        return $self->{sourceConfig}->{databaseName};
      }
    } else {
      return "NA";
    }
}


# Procedure getSourceName
# parameters: none
# Return database instance name

sub getSourceName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getSourceName",1);
    return $self->{source}->{name};
}

# Procedure getSourceConfigName
# parameters: none
# Return name of source config

sub getSourceConfigName
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getSourceConfigName",1);
    my $ret;
    if ($self->{sourceConfig} ne 'NA') {
        $ret = $self->{sourceConfig}->{name};
    }
    return $ret;
}

# Procedure getSourceConfigType
# parameters: none
# Return sourceconfig type (type of database, ex. Oracle SI, Oracle PDB)

sub getSourceConfigType
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getSourceConfigType",1);
    my $ret;
    if ($self->{sourceConfig} ne 'NA') {
        $ret = $self->{sourceConfig}->{type};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getHost
# parameters: none
# Return database hostname

sub getHost
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getHost",1);
    return $self->{host}->{name};
}

# Procedure getType
# parameters: none
# Return database type (dSource / VDB)

sub getType
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getType",1);
    my $type = $self->{source}->{type};

    if (defined($type)) {
        if ($type =~ /Linked(.*)Source/ ) {
            return "dSource";
        } else {
            return "VDB";
        }
    } else {
        return "detached";
    }
}

# Procedure getBackup`
# parameters:
# -engine
# -output
# Return a definition of backup metadata

sub getBackup
{
    my $self = shift;
    my $engine = shift;
    my $output = shift;
    my $dsource_output = shift;
    my $backup = shift;
    my $groupname = shift;
    my $parentname = shift;
    my $parentgroup = shift;
    my $templates = shift;
    my $groups = shift;
    logger($self->{_debug}, "Entering VDB_obj::getBackup",1);

    #my $hooks = new Hook_obj (  $self->{_dlpxObject}, 1, $self->{_debug} );
    #$self->{_hooks} = $hooks;

    if ($self->getType() eq 'VDB') {
      $self->getVDBBackup($engine, $output, $backup, $groupname, $parentname, $parentgroup, $templates, $groups);
    } elsif (($self->getType() eq 'dSource') || ($self->getType() eq 'detached')) {
      $self->getdSourceBackup($engine, $dsource_output, $backup, $groupname );
    }
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
    my $groups = shift;
    logger($self->{_debug}, "Entering VDB_obj::getVDBBackup",1);

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
      # is parent deleted ? can happen with replication
      $parentname = "PARENTDELETED";
      $parentgroup = "PARENTDELETED";
      logger($self->{_debug}, "Parent deleted for VDB - replication ?",2);
      print "There is no parent for VDB. It can happen if replicated objects are deleted. Parent name is set to PARENTDELETED\n";
    }

    $restore_args = "dx_provision_vdb$suffix -d $engine -type $vendor -group \"$groupname\" -creategroup";
    $restore_args = $restore_args . " -sourcename \"$parentname\"  -srcgroup \"$parentgroup\" -targetname \"$dbn\" ";
    $restore_args = $restore_args . " -dbname \"$dbhostname\" -environment \"" . $self->getEnvironmentName() . "\" ";
    $restore_args = $restore_args . " -envinst \"$rephome\" ";


    $restore_args = $restore_args . " -envUser \"" . $self->getEnvironmentUserName() . "\" ";
    $restore_args = $restore_args . " -hooks " . File::Spec->catfile($backup,$dbn.'.dbhooks') . " ";

    $restore_args = $restore_args . $self->getConfig($templates, 1, $groups);

    $output->addLine(
      $restore_args
    );

}


# Procedure getdSourceBackup`
# parameters:
# -engine
# -output
# -backup - location for hooks
# -groupname
# -parentname
# -parentgroup

# Return a definition of backup metadata

sub getdSourceBackup
{
    my $self = shift;
    my $engine = shift;
    my $output = shift;
    my $backup = shift;
    my $groupname = shift;

    logger($self->{_debug}, "Entering VDB_obj::getdSourceBackup",1);

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

    my $restore_args = "dx_ctl_dsource$suffix -d $engine -action create -group \"$groupname\" -creategroup ";
    $restore_args = $restore_args . "-dsourcename \"$dbn\"  -type $vendor -sourcename \"$dbhostname\" ";
    $restore_args = $restore_args . "-sourceinst \"$rephome\" -sourceenv \"" . $self->getEnvironmentName() . "\" -source_os_user \"$osuser\" ";

    my $logsync = $self->getLogSync() eq 'ACTIVE'? 'yes' : 'no' ;
    my $dbuser = $self->getDbUser();

    $restore_args = $restore_args . "-dbuser $dbuser -password ChangeMeDB -logsync $logsync";

    $restore_args = $restore_args . $self->getConfig(undef, 1);

    $output->addLine(
      $restore_args
    );
}


# Procedure getCurrentTimeflow
# parameters: none
# Return timeflow of the database

sub getCurrentTimeflow
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getCurrentTimeflow",1);
    return $self->{container}->{currentTimeflow};
}


# Procedure getStagingEnvironment
# parameters: none
# Return database staging environment

sub getStagingEnvironment
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getStagingEnvironment",1);
    my $ret;
    if (defined($self->{staging_environment}->{name})) {
      $ret = $self->{staging_environment}->{name};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getStagingInst
# parameters: none
# Return database staging environment

sub getStagingInst
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getStagingInst",1);
    my $ret;
    if (defined($self->{staging_repository}->{name})) {
      $ret = $self->{staging_repository}->{name};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}


# Procedure getStagingHost
# parameters: none
# Return database staging hostname

sub getStagingHost
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getStagingHost",1);
    return $self->{staging_host}->{name};
}

# Procedure getRuntimeStatus
# parameters: none
# Return database runtime status

sub getRuntimeStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getRuntimeStatus",1);
    my $ret;
    if (defined($self->{source}->{runtime})) {
        $ret = $self->{source}->{runtime}->{status};
    } else {
        $ret = 'NA';
    }
    return $ret;
}

# Procedure getRuntimeSize
# parameters: none
# Return database runtime size in GB

sub getRuntimeSize
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getRuntimeSize",1);
    my $ret;
    if (defined($self->{source}->{runtime})) {
        $ret = sprintf("%.2f",$self->{source}->{runtime}->{databaseSize} / 1024 / 1024 / 1024);
    } else {
        $ret = 'NA';
    }
    return $ret;
}

# Procedure refreshRuntime
# parameters:
# - Source_obj with new data

sub refreshRuntime
{
    my $self = shift;
    my $source = shift;
    logger($self->{_debug}, "Entering VDB_obj::refreshRuntime",1);
    $self->{"source"}  = $source->getSource($self->{container}->{reference});
}


# Procedure getEnabled
# parameters: none
# Return information if database is enabled or disabled

sub getEnabled
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getEnabled",1);

    my $ret;

    if ( $self->getType() eq 'detached' ) {

        $ret = 'N/A';

    } else {

        if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.5.0)) {
            $ret = $self->{source}->{enabled} ? "enabled" : "disabled";
        } else {
            $ret = ($self->{source}->{runtime}->{enabled} eq 'ENABLED')  ? "enabled" : "disabled"
        }
    }

    return $ret;
}

# Procedure detach_dsource
# Return job number if job started or undef otherwise

sub detach_dsource
{
    my $self = shift;

    logger($self->{_debug}, "Entering VDB_obj::detach_dsource",1);

    if ($self->getType() eq 'detached') {
        print "dSource is already detached\n";
        return undef;
    }


    my %detach_data = (
        "type" => "DetachSourceParameters",
        "source" =>  $self->{source}->{reference}
    );

    my $operation = 'resources/json/delphix/database/'. $self->{container}->{reference} .'/detachSource' ;
    my $json_data = encode_json(\%detach_data);
    return $self->runJobOperation($operation,$json_data, 'ACTION');
}

# Procedure return_currentobj
# Return current object

sub return_currentobj
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::return_currentobj",1);
    return $self->{_currentobj};
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


    logger($self->{_debug}, "Entering VDB_obj::update_dsource",1);

    my %source_hash;
    my $jobno;

    my $update = 0;
    my $dbtype = $self->getDBType();

    %source_hash = (
        "type" => $self->{source}->{type}
    );

    if ( ($dbtype eq 'mssql') || ($dbtype eq 'sybase') ) {

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
    }

    if ($update eq 1) {
      my $json_data = to_json(\%source_hash);

      logger($self->{_debug}, $json_data ,2);

      my $operation = 'resources/json/delphix/source/' . $self->{source}->{reference};
      $jobno = $self->runJobOperation($operation, $json_data, 'ACTION');
    } else {
      print "Nothing to update\n";
    }

    return $jobno;

}



# Procedure runJobOperation
# parameters:
# - operation - API string
# - json_data - JSON encoded data
# Run POST command running background job for particular operation and json data
# Return job number if job started or undef otherwise

sub runJobOperation {
    my $self = shift;
    my $operation = shift;
    my $json_data = shift;
    my $action = shift;

    logger($self->{_debug}, "Entering VDB_obj::runJobOperation",1);
    logger($self->{_debug}, $operation, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $jobno;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->{_currentobj} = $result->{result};
        if (defined($action) && $action eq 'ACTION') {
            $jobno = $result->{action};
        } else {
            $jobno = $result->{job};
        }
    } else {
        if (defined($result->{error})) {
            print "Problem with starting job\n";
            print "Error: " . Toolkit_helpers::extractErrorFromHash($result->{error}->{details}) . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, "error " . Dumper $result->{error}->{details},1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    return $jobno;
}

# Procedure start
# parameters: none
# Start VDB
# Return job number if job started or undef otherwise

sub start
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::start",1);
    my $operation = "resources/json/delphix/source/" . $self->{source}->{reference} . "/start";
    return $self->runJobOperation($operation,"{}");
}

# Procedure stop
# parameters: none
# Stop VDB
# Return job number if job started or undef otherwise

sub stop
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::stop",1);
    my $operation = "resources/json/delphix/source/" . $self->{source}->{reference} . "/stop";
    return $self->runJobOperation($operation,"{}");
}

# Procedure enable
# parameters: none
# Enable database
# Return job number if job started or undef otherwise

sub enable
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::enable",1);
    my $operation = "resources/json/delphix/source/" . $self->{source}->{reference} . "/enable";
    return $self->runJobOperation($operation,"{}");
}

# Procedure disable
# parameters:
# - force
# - type
# Disable database
# Return job number if job started or undef otherwise

sub disable
{
    my $self = shift;
    my $force = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering VDB_obj::disable",1);

    my $disable_force;

    if (defined($force)) {
        $disable_force = JSON::false;
    } else {
        $disable_force = JSON::true;
    };

    my %disable_hash;

    if (defined($type)) {
        %disable_hash = (
            type => $type,
            attemptCleanup => $disable_force
        );
    } else {
        %disable_hash = (
            type => "SourceDisableParameters",
            attemptCleanup => $disable_force
        );
    };


    my $json_data = encode_json(\%disable_hash);
    my $operation = "resources/json/delphix/source/" . $self->{source}->{reference} . "/disable";
    return $self->runJobOperation($operation,$json_data);
}

# Procedure delete
# parameters:
# - force
# - type
# Delete VDB
# Return job number if job started or undef otherwise

sub delete
{
    my $self = shift;
    my $force = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering VDB_obj::delete",1);
    my $delete_force;

    if (defined($force)) {
        $delete_force = JSON::true;
    } else {
        $delete_force = JSON::false;
    };

    my %delete_hash;

    if (defined($type)) {
        %delete_hash = (
            type => $type,
            force => $delete_force
        );
    } else {
        %delete_hash = (
            type => "DeleteParameters",
            force => $delete_force
        );
    };


    my $json_data = encode_json(\%delete_hash);
    my $operation = "resources/json/delphix/database/" . $self->{container}->{reference} . "/delete";
    #print Dumper $json_data;
    return $self->runJobOperation($operation,$json_data);
}


# Procedure snapshot
# parameters:
# - snapshot type hash
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot
{
    my $self = shift;
    my $snapshot_type = shift;

    logger($self->{_debug}, "Entering VDB_obj::snapshot",1);
    my $operation = "resources/json/delphix/database/" . $self->{container}->{reference} . "/sync";

    my $json_data = encode_json($snapshot_type);
    return $self->runJobOperation($operation,$json_data);
}


# Procedure rewind
# parameters:
# - timestamp - timestamp / LATEST_POINT / LATEST_SNAPSHOT
# - type - timeflow type
# rewind VDB
# Return job number if job started or undef otherwise

sub rewind
{
    my $self = shift;
    my $timestamp = shift;
    my $changenum = shift;
    my $type = shift;

    logger($self->{_debug}, "Entering VDB_obj::rewind",1);
    my $operation = "resources/json/delphix/database/" . $self->{container}->{reference} . "/rollback";

    if (! defined($type) ) {
        $type = 'RollbackParameters';
    }

    $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"} = $self->{container}->{reference};
    if (defined($timestamp)) {
        if ($self->setTimestamp($timestamp)) {
            print "Error with setting point in time for rewind \n";
            exit 1;
        }
    } elsif (defined($changenum)) {
        if ($self->setChangeNum($changenum)) {
            print "Error with setting location for rewind\n";
            exit 1;
        }
    } else {
        print "Point in time not defined\n";
        exit 1;
    }

    my %timeflow = (
        "type" => $type,
        "timeflowPointParameters" => $self->{"NEWDB"}->{"timeflowPointParameters"}
    );

    my $json_data = encode_json(\%timeflow);
    return $self->runJobOperation($operation,$json_data);
}


# Procedure refresh
# parameters:
# - timestamp - timestamp / LATEST_POINT / LATEST_SNAPSHOT
# - type - timeflow type
# refresh VDB
# Return job number if job started or undef otherwise

sub refresh
{
    my $self = shift;
    my $timestamp = shift;
    my $changenum = shift;
    my $type = shift;

    if (! defined($type) ) {
        $type = 'RefreshParameters';
    }

    logger($self->{_debug}, "Entering VDB_obj::refresh",1);
    my $operation = "resources/json/delphix/database/" . $self->{container}->{reference} . "/refresh";

    if (defined($self->{container}->{provisionContainer})) {
      $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"} = $self->{container}->{provisionContainer};
    } else {
      print "Parent database not found.\n";
      return undef;
    }



    if (defined($timestamp)) {
        if ($self->setTimestamp($timestamp)) {
            print "Error with setting point in time for refresh \n";
            return undef;
        }
    } elsif (defined($changenum)) {
        if ($self->setChangeNum($changenum)) {
            print "Error with setting location for refresh \n";
            return undef;
        }
    } else {
        print "Point in time not defined\n";
        return undef;
    }

    my %timeflow = (
        "type" => $type,
        "timeflowPointParameters" => $self->{"NEWDB"}->{"timeflowPointParameters"}
    );

    my $json_data = encode_json(\%timeflow);
    #print Dumper $json_data;
    return $self->runJobOperation($operation,$json_data);
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

# Procedure setHome
# parameters:
# - name - home name
# Set home/mssql instance reference by name for new db. Home/instance has to exist on defined environment
# Return 0 if success, 1 if not found

sub setHome {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering VDB_obj::setHome",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};
    my $repositories;

    if (defined($self->{_repository})) {
        $repositories = $self->{_repository};
    } else {
        $repositories = new Repository_obj($dlpxObject, $debug);
        $self->{_repository} = $repositories;
    }
    my $repitem = $repositories->getRepositoryByNameForEnv($name, $self->{'_newenv'});

    if (defined ($repitem)) {
        $self->{"NEWDB"}->{"sourceConfig"}->{"repository"} = $repitem->{'reference'};
        return 0;
    }  else {
        return 1;
    }

}


# Procedure getHome
# parameters:
# Return OH/instance name

sub getHome {
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getHome",1);

    my $name;

    if ( defined($self->{"repository"}) && ( $self->{"repository"} ne 'NA' ) ) {
        $name = defined($self->{"repository"}->{"name"}) ? $self->{"repository"}->{"name"} : 'N/A';
    } else {
        $name = 'N/A';
    }

    return $name;

}

# Procedure getVersion
# parameters:
# Return db version

sub getVersion {
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getVersion",1);

    my $version;
    if ( defined($self->{"repository"}) && ( $self->{"repository"} ne 'NA' ) ) {
        $version = defined($self->{"repository"}->{"version"}) ? $self->{"repository"}->{"version"} : 'N/A';
    } else {
        $version = 'N/A';
    }

    return $version;

}



# Procedure setConfig
# parameters:
# - name - source name
# Return SourceConfig reference

sub setConfig {
    my $self = shift;
    my $name = shift;
    my $source_inst = shift;
    my $source_env = shift;

    logger($self->{_debug}, "Entering VDB_obj::setConfig",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};

    my $sourceconfig;


    if (!defined($self->{_sourceconfig})) {
        $sourceconfig = new SourceConfig_obj($dlpxObject, $debug);
        $self->{_sourceconfig} = $sourceconfig;
    }

    my $ret;

    if (defined($source_inst) && defined($source_env)) {
    # this is for non unique souce name

        my $environments;
        if (defined($self->{_environment})) {
            $environments = $self->{_environment};
        } else {
            $environments = new Environment_obj($dlpxObject, $debug);
            $self->{_environment} = $environments;
        }

        my $envitem = $environments->getEnvironmentByName($source_env);

        #print Dumper $envitem;

        if (!defined($envitem)) {
            print "Can't find source environment - $source_env. Exiting\n";
            return undef;
        }

        my $repositories;

        if (defined($self->{_repository})) {
            $repositories = $self->{_repository};
        } else {
            $repositories = new Repository_obj($dlpxObject, $debug);
            $self->{_repository} = $repositories;
        }
        my $repitem = $repositories->getRepositoryByNameForEnv($source_inst, $envitem->{reference} );

        if (!defined($repitem)) {
            print "Can't find source home / instance - $source_inst. Exiting\n";
            return undef;
        }

        #print Dumper $self->{_sourceconfig};
        #print Dumper $repitem;

        $ret = $self->{_sourceconfig}->getSourceConfigByNameForRepo($name, $repitem->{reference});

    } else {
        # unique source config like for Oracle with db unique name
        $ret = $self->{_sourceconfig}->getSourceConfigByName($name);
    }

    return $ret;

}




# Procedure setGroup
# parameters:
# - name - group name
# Set target group name reference by name for new db.
# Return 0 if success, 1 if not found

sub setGroup {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering VDB_obj::setGroup",1);
    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};
    my $groups = new Group_obj($dlpxObject, $debug);
    $self->{_groups} = $groups;


    if (defined ($groups->getGroupByName($name))) {
        $self->{"NEWDB"}->{"container"}->{"group"} = $groups->getGroupByName($name)->{'reference'};
        return 0;
    } else {
        return 1;
    }

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
    logger($self->{_debug}, "Entering VDB_obj::setCredentials",1);

    if ($self->{_sourceconfig}->setCredentials($self->{sourceConfig}->{reference}, $username, $password, $force)) {
        print "Username or password is invalid.\n";
        return 1;
    } else {
        return 0;
    }

}


# Procedure setTimestamp
# parameters:
# - timestamp - timestamp / LATEST_POINT / LATEST_SNAPSHOT
# Set timestamp object for new db.
# Return 0 if success, 1 if not found

sub setTimestamp {
    my $self = shift;
    my $timestamp = shift;

    logger($self->{_debug}, "Entering VDB_obj::setTimestamp", 1);
    logger($self->{_debug}, "timestamp parameter " . $timestamp, 2);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};

    my $source_temp;


    if (! defined($self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}) ) {
        return 1;
    } else {
        $source_temp = $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};

    }

    my $snapshot = new Snapshot_obj($dlpxObject, $source_temp, undef);

    if ( $timestamp eq 'LATEST_SNAPSHOT') {
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"} = "LATEST_SNAPSHOT";
    }
    elsif ( $timestamp eq 'LATEST_POINT') {
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"} = "LATEST_POINT";
    }
    elsif ( (my ($year,$mon,$day,$hh,$mi,$ss) = $timestamp =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d?\d):(\d\d)/ ) ) {
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
        my $tz = new Date::Manip::TZ;

        my $fixformat_timestamp = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d", $year, $mon, $day, $hh, $mi, $ss);

        my $tf = $snapshot->findTimeflowforTimestamp($fixformat_timestamp);

        if (! defined($tf->{timezone})) {
            print "Can't find timeflow for point in time recovery. Check if timestamp is in provisioning range \n";
            return 1;
        }

        my $sttz = Toolkit_helpers::convert_to_utc($fixformat_timestamp, $tf->{timezone}, undef, 1);

        logger($self->{_debug}, "timeflow - " . $tf->{timeflow} . " -  requested timestamp - " . $sttz ,2);

        if ($sttz lt $tf->{full_startpoint}) {
          # if real subseconds are bigger than 000 we need to use real subseconds. This is an issue for AppData
          $sttz = $tf->{full_startpoint};
        }

        logger($self->{_debug}, "timestamp after check - " . $sttz ,2);

        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointTimestamp";
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timeflow"} = $tf->{timeflow};
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timestamp"} = $sttz;

    }
    elsif ( ( ($year,$mon,$day,$hh,$mi) = $timestamp =~ /^(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d\d)$/ ) ) {
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
        my $tz = new Date::Manip::TZ;

        my $fixformat_timestamp = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d", $year, $mon, $day, $hh, $mi);

        my $tf = $snapshot->findSnapshotforTimestamp($fixformat_timestamp);

        if (! defined($tf->{timezone})) {
            print "Can't find snapshot specified by timestamp. Check list of snapshots. \n";
            return 1;
        }

        my $sttz = Toolkit_helpers::convert_to_utc($fixformat_timestamp, $tf->{timezone}, undef, 1);

        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointTimestamp";
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timeflow"} = $tf->{timeflow};
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timestamp"} = $tf->{timestamp};

        logger($self->{_debug}, "timeflow - " . $tf->{timeflow} . " -  timestamp - " . $tf->{timestamp} ,2);


    }
    elsif ( $timestamp =~ /^@\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d\d\d.?$/ )  {
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
        my $tz = new Date::Manip::TZ;


        my $snaparray = $snapshot->getSnapshotByName($timestamp);

        if (scalar(@{$snaparray}) eq 0 ) {
            print "Snapshot name for a VDB not found. \n";
            return 1;
        }


        if (scalar(@{$snaparray}) > 1 ) {
            print "More than one snapshot returned by name. Double check snapshot name. \n";
            return 1;
        }

        my $snapref = $snaparray->[0];
        my $snapshot_time = $snapshot->getStartPoint($snapref);
        my $snapshot_timeflow = $snapshot->getSnapshotTimeflow($snapref);


        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointTimestamp";
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timeflow"} = $snapshot_timeflow;
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timestamp"} = $snapshot_time;

        logger($self->{_debug}, "timeflow - " . $snapshot_timeflow . " -  timestamp - " . $snapshot_time ,2);
    }
    elsif ( $timestamp eq 'LATEST_PROVISIONABLE_SNAPSHOT' )  {
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
        delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
        my $tz = new Date::Manip::TZ;

        my $snapref = $snapshot->getLastProvisionableSnapshot();

        if (!defined($snapref)) {
          print "There is no provisionable snapshot found.\n";
          return 1;
        }

        my $snapshot_time = $snapshot->getStartPoint($snapref);
        my $snapshot_timeflow = $snapshot->getSnapshotTimeflow($snapref);


        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointTimestamp";
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timeflow"} = $snapshot_timeflow;
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timestamp"} = $snapshot_time;

        logger($self->{_debug}, "timeflow - " . $snapshot_timeflow . " -  timestamp - " . $snapshot_time ,2);
    }
    else {
        my $bookmarks = new Bookmark_obj ($self->{_dlpxObject}, undef, $self->{_debug});
        my $bookmark = $bookmarks->getBookmarkByName($timestamp);
        if (defined($bookmark)) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointBookmark";
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"bookmark"} = $bookmark->{reference};
            delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
            delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
        } else {
            print "Timestamp $timestamp format doesn't match any known format \n";
            return 1;
        }
    }

    return 0;

}




# Procedure setChangeNum
# parameters:
# - changenum
# Set changenum object for new db.
# Return 0 if success, 1 if not found

sub setChangeNum {
    my $self = shift;
    my $changenum = shift;

    logger($self->{_debug}, "Entering VDB_obj::setChangeNum",1);


    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};


    my $source_temp;


    if (! defined($self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}) ) {
        return 1;
    } else {
        $source_temp = $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
    }

    my $snapshot = new Snapshot_obj($dlpxObject, $source_temp, 1);


    my $tf = $snapshot->findTimeflowforLocation($changenum);

    if (! defined($tf)) {
        return 1;
    }

    delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
    delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
    $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointLocation";
    $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timeflow"} = $tf->{timeflow};
    $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"} = $changenum;

    return 0;

}

# Procedure setMapFileV2P
# parameters:
# - map_file - hash of map file
# Set mountpoint for new db.

sub setMapFileV2P {
    my $self = shift;
    my $map_file = shift;
    logger($self->{_debug}, "Entering VDB_obj::setMapFileV2P",1);
    $self->{"NEWDB"}->{"fileMappingRules"} = $map_file;
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

    logger($self->{_debug}, "Entering VDB_obj::setFileSystemLayout",1);

    $self->{"NEWDB"}->{"filesystemLayout"}->{"type"} = "TimeflowFilesystemLayout";

    if (! defined($targetDirectory)) {
        return 1;
    }

    $self->{"NEWDB"}->{"filesystemLayout"}->{"targetDirectory"} = $targetDirectory;

    if ( defined($archiveDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"archiveDirectory"} = $archiveDirectory;
    }

    if ( defined($tempDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"tempDirectory"} = $tempDirectory;
    }

    if ( defined($scriptDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"scriptDirectory"} = $scriptDirectory;
    }

    if ( defined($externalDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"externalDirectory"} = $externalDirectory;
    }

    if ( defined($dataDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"dataDirectory"} = $dataDirectory;
    }

}


# Procedure upgradeVDB
# parameters:
# - home - new DB home
# Upgrade VDB
# Return job number if provisioning has been started, otherwise return undef

sub upgradeVDB {
    my $self = shift;
    my $home = shift;
    my $type = shift;
    my $ret;


    logger($self->{_debug}, "Entering VDB_obj::upgradeVDB",1);

    my $env = $self->{environment}->{name};

    my $sourceconfig = $self->{sourceConfig}->{reference};

    $type = $self->{sourceConfig}->{type};

    if ( $self->setEnvironment($env) ) {
        print "Environment $env not found. VDB won't be upgraded\n";
        return undef;
    }

    if ( $self->setHome($home) ) {
        print "Home $home in environment $env not found. VDB won't be upgraded\n";
        return undef;
    }

    my %upgrade_hash = (
        type => $type,
        #environmentUser: "HOST_USER-2",
        repository => $self->{NEWDB}->{sourceConfig}->{repository}
    );

    my $json_data = encode_json(\%upgrade_hash);

    my $operation = 'resources/json/delphix/sourceconfig/' . $sourceconfig;

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        # # check action status
        # # get last hour of actions
        # my $st = Toolkit_helpers::timestamp("-5mins", $self->{_dlpxObject});
        # my $action = new Action_obj ($self->{_dlpxObject}, $st, undef, undef);
        # print "Waiting for all actions to complete. Parent action is " . $result->{action} . "\n";
        # if ( $action->checkStateWithChild($result->{action}) eq 'COMPLETED' ) {
        #     print "Upgrade completed with success.\n";
        #     $ret = 0;
        # } else {
        #     print "There were problems with upgrade.\n";
        #     $ret = 1;
        # }
        #

        $ret = Toolkit_helpers::waitForAction($self->{_dlpxObject}, $result->{action}, "Upgrade completed with success", "There were problems with upgrade.");

    } else {
        print "There were problems with upgrade.\n";
        if (defined($result->{error})) {
            print $result->{error}->{action} . "\n";
        }
        $ret = 1;
    }


    return $ret;

}

# Procedure getBCT
# parameters: none
# Return database bct information

sub getBCT
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getBCT",1);

    return 'N/A';
}

# Procedure getHook
# parameters:
# - hooktype - type of hook
# Return Hook body array

sub getHook {
    my $self = shift;
    my $hooktype = shift;
    my $save = shift;
    logger($self->{_debug}, "Entering VDB_obj::getHook",1);

    my $hook_hash = $self->{"source"}->{"operations"};

    my @retarray;

    if (defined($hook_hash->{$hooktype})) {

      my $count = 0;

      for my $i (@{$hook_hash->{$hooktype}}) {
        my %ret_hash;
        $ret_hash{hooktype} = $hooktype;
        $ret_hash{hookOSType} = $self->getHookOSType($i->{type});
        if (defined($save)) {
          $ret_hash{command} = $i->{command};
        } else {
          $ret_hash{command} = $self->getCommand($i->{command}, $i->{type});
        }
        $ret_hash{name} = $i->{name};
        $ret_hash{number} = $count;
        if (!defined($i->{name})) {
          $ret_hash{name}  = $count;
        }
        $count++;
        push(@retarray, \%ret_hash);
      }


    }

    return \@retarray;

}


# Procedure getHookOSType
# parameters:
# - val
# Return human hook type for specific hook internal type

sub getHookOSType {
    my $self = shift;
    my $val = shift;

    my $ret;

    if ($val eq 'RunBashOnSourceOperation') {
        $ret = 'BASH';
    } elsif ($val eq 'RunPowerShellOnSourceOperation') {
        $ret = 'PS';
    } elsif ($val eq 'RunExpectOnSourceOperation') {
        $ret = 'EXPECT';
    } elsif ($val eq 'RunCommandOnSourceOperation') {
        $ret = 'SHELL';
    } else {
        $ret = $val;
    }

    return $ret;
}

# Procedure getCommand
# parameters:
# - val
# - type
# Return hook command for specific hook with <cr>

sub getCommand {
    my $self = shift;
    my $ret = shift;
    my $type = shift;


    if ($type eq 'RunPowerShellOnSourceOperation') {
      $ret =~ s/\r\n/<cr>/g;
    } else {
      $ret =~ s/\n/<cr>/g;
    }
    return $ret;
}


# Procedure setHooksfromJSON
# parameters:
# - hook - JSON object
# Set Hook from JSON

sub setHooksfromJSON {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setHooksfromJSON",1);

    $self->{"NEWDB"}->{"source"}->{"operations"} = $hook;
}

# Procedure deleteHook
# parameters:
# - hooktype - type of hook
# - hookname
# delete from existing hooks list

sub deleteHook {
    my $self = shift;
    my $hooktype = shift;
    my $hookname = shift;
    logger($self->{_debug}, "Entering VDB_obj::deleteHook",1);

    $self->{"source"} = $self->{_source}->refreshSource($self->{"source"}->{"reference"});

    my @hook_array = @{$self->{"source"}->{"operations"}->{$hooktype}};

    if (scalar(@hook_array) eq 0) {
      #hook not found
      return 2;
    }

    my %hook_update_hash = (
      "type" => $self->{"source"}->{type},
      "operations" => {
        "type" => $self->{"source"}->{operations}->{type}
      }
    );

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
      # there is no name for hook so hook will be a number

      if ($hookname > scalar(@hook_array)) {
        # hook not found
        return 2;
      }

      splice @hook_array, $hookname, 1;


    } else {
      if (grep { $_->{name} eq $hookname } @hook_array) {
        @hook_array = grep { $_->{name} ne $hookname } @hook_array;
      } else {
        #hook not found
        return 2;
      }

    }

    $hook_update_hash{"operations"}{$hooktype} = \@hook_array;

    my $json_data = to_json(\%hook_update_hash);

    my $operation = 'resources/json/delphix/source/' . $self->{"source"}->{"reference"};

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {

        $ret = Toolkit_helpers::waitForAction($self->{_dlpxObject}, $result->{action}, "Hook deleted", "There were problems with hook deletion.");

    } else {
        print "There were problem with hook deletion action.\n";
        if (defined($result->{error})) {
            print $result->{error}->{action} . "\n";
        }
        $ret = 1;
    }

    #refresh source

    $self->{"source"} = $self->{_source}->refreshSource($self->{"source"}->{"reference"});

    return $ret;

}

# Procedure updateHook
# parameters:
# - hooktype - type of hook
# - hookname
# - hook
# - hook - shell command (line sepatated by /r)
# update existing hooks list

sub readHook {
    my $self = shift;
  	my $hooktype = shift;
  	my $hooklist = shift;
    my $op_templates = shift;
  	my $FD;

    my $ret = 0;

    for my $hookitem (@{$hooklist}) {
      my @linesplit = split(',',$hookitem);
      my $hookname;
      my $hookfilename;
      my $hookOStype;

      my $hookbody;


      if (scalar(@linesplit) > 1) {
        if (defined($linesplit[0])) {
          $hookname = $linesplit[0];
        }
        if (defined($linesplit[1])) {
          $hookfilename = $linesplit[1];
        }
        if (defined($linesplit[2])) {
          $hookOStype = $linesplit[2];
        } else {
          $hookOStype = "bash"; #default
        }
      } else {
        my @tf = File::Basename::fileparse($hookitem, ('.BASH','.SHELL','.EXPECT','.PS'));
        $hookname = $tf[0];
        $hookOStype = "bash";
        $hookfilename = $hookitem;
      }

      # if (scalar(@linesplit) > 2) {
      #   #hook in file with type
      #   $hookOStype = $linesplit[0];
      #   $hookname = $linesplit[1];
      #   if (! open ($FD, $linesplit[2])) {
      #     print "Can't open a file with $hookname script: $linesplit[2]\n";
      #     return undef;
      #   }
      #   my @script = <$FD>;
      #   close($FD);
      #   $hookbody = join('', @script);
      # } else {
        #hook in file or op template
        my $hookref = $op_templates->getHookByName($hookfilename);

        if (defined($hookref) && (-e $hookfilename)) {
          print "Hook filename match also operation template name\n";
          print "Please rename a file or operation template to have unique match\n";
          return undef;
        }

        if (defined($hookref)) {
          $hookbody = $op_templates->getHook($hookref)->{operation}->{command};
          $hookOStype = $op_templates->getType($hookref);
        } else {

          if (! open ($FD, $hookfilename)) {
            print "Can't open a file with $hookname script: $hookitem\n";
            return undef;
          }
          my @script = <$FD>;
          close($FD);
          $hookbody = join('', @script);

        }
      #}

      $ret = $ret + $self->setHook($hooktype, $hookOStype, $hookname, $hookbody);
    }

    return $ret;

}


# Procedure setHook
# parameters:
# - hooktype - type of hook
# - hook - shell command (line sepatated by /r)
# Set Hook

sub setHook {
    my $self = shift;
    my $hooktype = shift;
    my $ostype = shift;
    my $hookname = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setHook",1);

    my %hook_hash;
    my $ret = 0;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.5.0)) {
        %hook_hash = (
            "type" => "RunCommandOperation", # this is API 1.4
            "command" => $hook
        );
    } else {
        my $hookOStype;

        if (lc $ostype eq 'bash') {
          $hookOStype = 'RunBashOnSourceOperation';
        } elsif (lc $ostype eq 'shell') {
          $hookOStype = 'RunCommandOnSourceOperation';
        } elsif (lc $ostype eq 'expect') {
          $hookOStype = 'RunExpectOnSourceOperation';
        } elsif (lc $ostype eq 'ps') {
          $hookOStype = 'RunPowerShellOnSourceOperation';
        } else {
          $hookOStype = 'RunBashOnSourceOperation';
        }

        %hook_hash = (
            "type" => $hookOStype, # this is API > 1.4
            "command" => $hook
        );
    }

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.9.0)) {
      $hook_hash{"name"} = $hookname;
    }

    my @hook_array;




    if (defined($self->{"source"}->{"reference"})) {
      my %hook_update_hash = (
        "type" => $self->{"source"}->{type},
        "operations" => {
          "type" => $self->{"source"}->{operations}->{type}
        }
      );

      #refresh source to make sure we have a latest state

      $self->{"source"} = $self->{_source}->refreshSource($self->{"source"}->{"reference"});

      if (defined($self->{"source"}->{"operations"}->{$hooktype})) {
        @hook_array = @{$self->{"source"}->{"operations"}->{$hooktype}};
        # todo
        # add replace hook with same name or number

        if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
          # there is no name for hook so hook will be a number

          if ($hookname =~ /\D/) {
            $hookname = 1000;
          }

          if (scalar(@hook_array)<$hookname) {
            $hookname = scalar(@hook_array);
          }

          $hook_array[$hookname] = \%hook_hash;

        } else {
          if (grep { $_->{name} eq $hookname } @hook_array) {
            @hook_array = grep { $_->{name} ne $hookname } @hook_array;
          }
          push(@hook_array, \%hook_hash);

        }



      } else {
        @hook_array = ( \%hook_hash );
      }

      $hook_update_hash{"operations"}{$hooktype} = \@hook_array;

      my $json_data = to_json(\%hook_update_hash);

      my $operation = 'resources/json/delphix/source/' . $self->{"source"}->{"reference"};

      my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

      if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {

          $ret = Toolkit_helpers::waitForAction($self->{_dlpxObject}, $result->{action}, "Hook added", "There were problems with adding hook.");

      } else {
          print "There were problem with adding hook action.\n";
          if (defined($result->{error})) {
              print $result->{error}->{action} . "\n";
          }
          $ret = 1;
      }

      #refresh source

      $self->{"source"} = $self->{_source}->refreshSource($self->{"source"}->{"reference"});

    } else {
      if (defined($self->{"NEWDB"}->{"source"}->{"operations"}->{$hooktype})) {
        @hook_array = @{$self->{"NEWDB"}->{"source"}->{"operations"}->{$hooktype}};
        push(@hook_array, \%hook_hash);
      } else {
        @hook_array = ( \%hook_hash );
      }
      $self->{"NEWDB"}->{"source"}->{"operations"}->{$hooktype} = \@hook_array;
    }

    return $ret;

}


# Procedure setPostRefreshHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Post Refresh Hook

sub setAnyHook {
    my $self = shift;
    my $type = shift;
    my $hooks = shift;

    logger($self->{_debug}, "Entering VDB_obj::setAnyHook",1);

    my $op_templates;

    if (defined($self->{_op_templates})) {
      $op_templates  = $self->{_op_templates};
    } else {
      $op_templates = new Op_template_obj ( $self->{_dlpxObject}, undef, $self->{_debug});
    }

    my $ret = $self->readHook($type, $hooks, $op_templates);
    return $ret;
}

# Procedure setPostRefreshHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Pre Refresh Hook

sub setPostRefreshHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPostRefreshHook",1);

    $self->setAnyHook('postRefresh', $hook);
}

# Procedure setPreRefreshHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Pre Refresh Hook

sub setPreRefreshHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPreRefreshHook",1);

    $self->setAnyHook('preRefresh', $hook);
}

# Procedure setconfigureCloneHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Configure Clone hook

sub setconfigureCloneHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setconfigureClonehHook",1);

    $self->setAnyHook('configureClone', $hook);
}

# Procedure setpreRollbackHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Pre Rewind Hook

sub setPreRewindHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setpreRollbackHook",1);

    $self->setAnyHook('preRollback', $hook);
}

# Procedure setpostRollbackHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Post Rewind Hook

sub setPostRewindHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setpostRollbackHook",1);

    $self->setAnyHook('postRollback', $hook);
}

# Procedure setPreSnapshotHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Pre Snapshot Hook

sub setPreSnapshotHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPreSnapshotHook",1);

    $self->setAnyHook('preSnapshot', $hook);
}

# Procedure setPostSnapshotHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Post Snapshot Hook

sub setPostSnapshotHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPostSnapshotHook",1);

    $self->setAnyHook('postSnapshot', $hook);
}

# Procedure setPreStartHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Pre Start Hook

sub setPreStartHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPreStartHook",1);

    $self->setAnyHook('preStart', $hook);
}

# Procedure setPostStartHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Post Start Hook

sub setPostStartHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPostStartHook",1);

    $self->setAnyHook('postStart', $hook);
}

# Procedure setPreStopHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Pre stop Hook

sub setPreStopHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPreStopHook",1);

    $self->setAnyHook('preStop', $hook);
}

# Procedure setPostStopHook
# parameters:
# - hook - shell command (line sepatated by /r)
# Set Post Stop Hook

sub setPostStopHook {
    my $self = shift;
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPostStopHook",1);

    $self->setAnyHook('postStop', $hook);
}

# Procedure exportDBHooks
# parameters:
# - location - directory
# Return 0 if no errors

sub exportDBHooks {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering VDB_obj::exportDBHooks",1);

    my $hooks = $self->{source}->{operations};

    if (defined($hooks)) {
      my $dbname = $self->getName();
      my $filename =  $location . "/" . $dbname . ".dbhooks";
      print "Exporting database $dbname hooks into  $filename \n";
      $self->exportJSONHook($hooks, $filename);
    }

    return 0;
}


sub exportJSONHook {
    my $self = shift;
    my $hook = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering VDB_obj::exportJSONHook",1);

    open (my $FD, '>', "$location") or die ("Can't open file $location : $!");
    binmode($FD, ":encoding(UTF-8)");
    print $FD to_json($hook, {pretty => 1});

    close $FD;

}

sub exportHook {
    my $self = shift;
    my $body = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering VDB_obj::exportHook",1);

    open (my $FD, '>', "$location") or die ("Can't open file $location : $!");
    binmode($FD, ":encoding(UTF-8)");
    print $FD $body;
    close $FD;

}


# Procedure importDBHooks
# parameters:
# - database object
# - filename - filename
# Return 0 if no errors

sub importDBHooks {
    my $self = shift;
    my $dbobj = shift;
    my $filename = shift;

    logger($self->{_debug}, "Entering VDB_obj::importDBHooks",1);

    my $hooks = $dbobj->{source}->{operations};
    my $source = $dbobj->{source}->{reference};
    my $type = $dbobj->{source}->{type};
    my $dbname = $dbobj->getName();

    my $loadedHook;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");
    binmode($FD, ":encoding(UTF-8)");
    local $/ = undef;
    my $json = JSON->new();
    $loadedHook = $json->decode(<$FD>);

    close $FD;

    print "Importing hooks from $filename into database $dbname \n";

    my $operation = 'resources/json/delphix/source/' . $source;

    my %hooks_hash = (
        type => $type,
        operations => $loadedHook
    );

    my $json_data = to_json(\%hooks_hash);

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ($result->{status} eq 'OK') {
        print "Import completed\n";
        return 0;
    } else {
        return 1;
    }

    return 0;
}


#######################
# end of VDB_obj class
#######################

package MySQLVDB_obj;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering MySQLVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug);

   # MySQL specific properties


    my @configureClone;
    my @postRefresh;
    my @preRefresh;
    my %configParams = ();

    my %operations = (
        "type" => "VirtualSourceOperations",
        "configureClone" => \@configureClone,
        "postRefresh" => \@postRefresh,
        "preRefresh" => \@preRefresh
    );

    my %prov = (
            "type" => "MySQLProvisionParameters",
            "container" => {
                "type" => 'MySQLDatabaseContainer',
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
                    "type" => "MySQLServerConfig",
                    "repository" => ""
            },
            "source" => {
                    "type" => "MySQLVirtualSource",
                    "operations" => \%operations,
                    "configParams" => \%configParams,
            },
            "timeflowPointParameters" => {
                "type" => "TimeflowPointSemantic",
                "container" => "",
                "location" => "LATEST_SNAPSHOT"
            },
    );

    $self->{"NEWDB"} = \%prov;
    $self->{_dbtype} = 'mysql';
    return $self;
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
    my $port = shift;
    my $mountpoint = shift;

    logger($self->{_debug}, "Entering MySQLVDB_obj::createVDB",1);


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

    print Dumper $self->{"NEWDB"}->{"source"};

    delete $self->{"NEWDB"}->{"sourceConfig"}->{"linkingEnabled"};

    $self->setPort($port);
    $self->setMountPoint($mountpoint);

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.8.0)) {
      $self->{"NEWDB"}->{"source"}->{"allowAutoVDBRestartOnHostReboot"} = JSON::false;
    }

    my $operation = 'resources/json/delphix/database/provision';
    my $json_data = $self->getJSON();

    #print Dumper $json_data;

    return $self->runJobOperation($operation,$json_data);

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
    logger($self->{_debug}, "Entering MySQLVDB_obj::setName",1);

    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"databaseName"} = $dbname;

}

# Procedure setPort
# parameters:
# - port
# Set port


sub setPort {
    my $self = shift;
    my $port = shift;
    logger($self->{_debug}, "Entering MySQLVDB_obj::setPort",1);

    $self->{"NEWDB"}->{"sourceConfig"}->{"port"} = $port;
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
    logger($self->{_debug}, "Entering MySQLVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};


    if (defined ($sourceitem)) {
        my $sourcetype = $sourceitem->{container}->{'type'};

        if (($sourcetype eq 'MySQLDatabaseContainer') || ($sourcetype eq 'MySQLVirtualSource')) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}  = $sourceitem->{container}->{reference};
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }

}


# Procedure setMountPoint
# parameters:
# - mountpoint - mount point
# Set mountpoint for new db.

sub setMountPoint {
    my $self = shift;
    my $mountpoint = shift;
    logger($self->{_debug}, "Entering MySQLVDB_obj::setMountPoint",1);

    my $mntpoint;

    if (defined($mountpoint)) {
        $mntpoint = $mountpoint;
    } else {
    # propose default from toolkit location / provision / container_name
        if (! defined($self->{'_hosts'})) {
            print "Host is not set. Can't create a default mountpoint";
            exit 1;
        }


        my $hosts = new Host_obj ($self->{_dlpxObject}, $self->{_debug});
        my $toolkitpath = $hosts->getToolkitpath($self->{'_hosts'});

        $mntpoint = $toolkitpath . "/provision/" . $self->{"NEWDB"}->{container}->{name};

    }
    $self->{"NEWDB"}->{"source"}->{"mountBase"} = $mntpoint;
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
    logger($self->{_debug}, "Entering MySQLVDB_obj::snapshot",1);

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
            "type" => "MySQLNewMySQLDumpSyncParameters"
        );
    }
    else {
        %snapshot_type = (
            "type" => "MySQLNewMySQLDumpSyncParameters",
            "loadFromBackup" => $frombackup_json
        );
    }
    return $self->VDB_obj::snapshot(\%snapshot_type) ;
}



########################

package PostgresVDB_obj;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering PostgresVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug);

    $self->{_dbtype} = 'postgresql';

    return $self;
}

#
# End of package


1;
