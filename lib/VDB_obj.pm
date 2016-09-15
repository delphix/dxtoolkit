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

use Group_obj;
use Host_obj;
use Source_obj;
use Snapshot_obj;
use Action_obj;
use Namespace_obj;
use Bookmark_obj;
use SourceConfig_obj;
use Environment_obj;
use Repository_obj;
use Toolkit_helpers qw (logger);

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

# Procedure getDbUser
# parameters: none
# Return database user

sub getDbUser 
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getDbUser",1);
    my $ret;
    if ($self->{sourceConfig} ne 'NA') {
      $ret = $self->{sourceConfig}->{user}
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
      $ret = $self->{_environment}->getEnvironmentUserByRef($envref, $user);
    } else {
      $ret = 'N/A';
    }
    
    return $ret;
}

# Procedure getOSUser
# parameters: none
# Return OS user

sub getStagingUser 
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getOSUser",1);
    my $ret;
    my $user;
    
    my $staging_env = $self->{staging_environment}->{reference};
    my $staging_user_ref = $self->{staging_sourceConfig}->{environmentUser};
    $ret = $self->{_environment}->getEnvironmentUserByRef($staging_env, $staging_user_ref);

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
        $ret = $self->{_environment}->getEnvironmentUserByRef($self->{environment}->{reference}, $self->{sourceConfig}->{environmentUser});
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

    my $ret;

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
    return $self->{sourceConfig}->{databaseName};
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
    return $self->{staging_environment}->{name};
}

# Procedure getStagingInst
# parameters: none
# Return database staging environment

sub getStagingInst 
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getStagingInst",1);
    return $self->{staging_repository}->{name};
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

        if ($self->{_dlpxObject}->getApi() lt "1.5") {
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
    my $groups = new Group_obj($dlpxObject);    

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
    
    logger($self->{_debug}, "Entering VDB_obj::setTimestamp",1);

    #print Dumper $timestamp;
    #print Dumper $timezone;


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

        my $dt = ParseDate($fixformat_timestamp);

        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $tf->{timezone});

        my $sttz = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);


        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointTimestamp";
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timeflow"} = $tf->{timeflow};
        $self->{"NEWDB"}->{"timeflowPointParameters"}->{"timestamp"} = $sttz;

        logger($self->{_debug}, "timeflow - " . $tf->{timeflow} . " -  timestamp - " . $sttz ,2);

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

        my $dt = ParseDate($fixformat_timestamp);

        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $tf->{timezone});

        my $sttz = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);


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
    else {
        my $bookmarks = new Bookmark_obj ($self->{_dlpxObject}, undef, $self->{_debug});
        my $bookmark = $bookmarks->getBookmarkByName($timestamp);
        if (defined($bookmark)) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"type"} = "TimeflowPointBookmark";
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"bookmark"} = $bookmark->{reference};
            delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"location"};
            delete $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"};
        } else {
            print "Timestamp format doesn't match any known format \n";
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
        # check action status
        # get last hour of actions
        my $st = Toolkit_helpers::timestamp($self->{_dlpxObject}->getTime(5), $self->{_dlpxObject});
        my $action = new Action_obj ($self->{_dlpxObject}, $st, undef, undef);
        print "Waiting for all actions to complete. Parent action is " . $result->{action} . "\n";
        if ( $action->checkStateWithChild($result->{action}) eq 'COMPLETED' ) {
            print "Upgrade completed with success.\n";
            $ret = 0;
        } else {
            print "There were problems with upgrade.\n";
            $ret = 1;
        }
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


# Procedure setPostRefreshHook
# parameters: 
# - hook - shell command (line sepatated by /r)
# Set Post Refresh Hook

sub setPostRefreshHook {
    my $self = shift; 
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setPostRefreshHook",1);

    my %hook_hash;

    if ($self->{_dlpxObject}->getApi() lt "1.5") {
        %hook_hash = (
            "type" => "RunCommandOperation", # this is API 1.4
            "command" => $hook
        );
    } else {
        %hook_hash = (
            "type" => "RunCommandOnSourceOperation", # this is API > 1.4
            "command" => $hook
        );
    }
    my @hook_array = ( \%hook_hash );
    $self->{"NEWDB"}->{"source"}->{"operations"}->{"postRefresh"} = \@hook_array;
}  


# Procedure setPostRefreshHook
# parameters: 
# - hook - shell command (line sepatated by /r)
# Set Post Refresh Hook

sub setconfigureCloneHook {
    my $self = shift; 
    my $hook = shift;
    logger($self->{_debug}, "Entering VDB_obj::setconfigureClonehHook",1);

    my %hook_hash;

    if ($self->{_dlpxObject}->getApi() lt "1.5") {
        %hook_hash = (
            "type" => "RunCommandOperation", # this is API 1.4
            "command" => $hook
        );
    } else {
        %hook_hash = (
            "type" => "RunCommandOnSourceOperation", # this is API > 1.4
            "command" => $hook
        );
    }
    my @hook_array = ( \%hook_hash );
    $self->{"NEWDB"}->{"source"}->{"operations"}->{"configureClone"} = \@hook_array;
}  

#######################
# end of VDB_obj class
#######################

# class OracleVDB_obj - is a child class of VDB_obj

package OracleVDB_obj;
use Data::Dumper;
use Template_obj;
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
    logger($debug,"Entering OracleVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug); 
    
    # Oracle specific properties - set to default for new DB
    my @services ;
    my @nodeListenerList ;
    my @configureClone;
    my @postRefresh;
    my @preRefresh;
    my %configParams = (
        "processes" => "150",
        "open_cursors" => "300",
        "remote_login_passwordfile" => "EXCLUSIVE"
    );

    my %operations = (
        "type" => "VirtualSourceOperations",
        "configureClone" => \@configureClone,
        "postRefresh" => \@postRefresh,
        "preRefresh" => \@preRefresh
    );

    my %prov = (
            "type" => "OracleProvisionParameters",
            "container" => {
                "type" => 'OracleDatabaseContainer',
                "name" => '',
                "group" => '',
                #"masked" => JSON::false,
                #performanceMode" => JSON::false,
                "sourcingPolicy" => {
                    "type" => 'OracleSourcingPolicy',
                    "loadFromBackup" => JSON::false,
                    "logsyncEnabled" => JSON::false,
                    "logsyncInterval" => 5,
                    "logsyncMode" => "UNDEFINED"
                }
            },
            "sourceConfig" => {
                    "type" => "OracleSIConfig",
                    "services" => \@services,
                    "linkingEnabled" => JSON::false,
                    "repository" => "",
                    "databaseName" => "",
                    "uniqueName" => "",
                    "instance" => {
                        "type" => "OracleInstance",
                        "instanceName" => "",
                        "instanceNumber" => 1
                    }
            },
            "source" => {
                    "type" => "OracleVirtualSource",
                    "configParams" => \%configParams,
                    "mountBase" => "/mnt/provision",
                    "nodeListenerList" => \@nodeListenerList,
                    "operations" => \%operations,
            },
            "timeflowPointParameters" => {
                "type" => "TimeflowPointSemantic",
                "container" => "",
                "location" => "LATEST_SNAPSHOT"
            }
    );

    $self->{"NEWDB"} = \%prov;
    $self->{_dbtype} = 'oracle';

    return $self;
}


# Procedure getInstances
# parameters: none
# Return database instance information

sub getInstances 
{
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getInstances",1);
    my @temparr;
    my $ret;


    if ( (defined($self->{sourceConfig}) ) && ($self->{sourceConfig} ne 'NA') ) {
        if (defined($self->{sourceConfig}->{instance})) {
            push (@temparr, $self->{sourceConfig}->{instance});
            $ret = \@temparr;
        } elsif (defined($self->{sourceConfig}->{instances})) {
            $ret = $self->{sourceConfig}->{instances}
        } else {
            $ret = 'UNKNOWN';
        }
    } else {
        $ret = 'UNKNOWN';
    }

    return $ret;
}

# Procedure getInstanceNode
# parameters: 
# - instanceNumber
# Return instance host

sub getInstanceNode 
{
    my $self = shift;
    my $instanceNumber = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getInstanceHost",1);

    my $ret;

    if (defined($self->{instances}->{$instanceNumber})) {
        $ret = $self->{instances}->{$instanceNumber}->{nodename};
    } else {
        $ret = 'UNKNOWN';
    }

    return $ret;
}


# Procedure getInstanceHost
# parameters: 
# - instanceNumber
# Return instance host

sub getInstanceHost 
{
    my $self = shift;
    my $instanceNumber = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getInstanceHost",1);

    my $ret;

    if (defined($self->{instances}->{$instanceNumber})) {
        $ret = $self->{instances}->{$instanceNumber}->{host};
    } else {
        $ret = 'UNKNOWN';
    }

    return $ret;
}

# Procedure getInstanceStatus
# parameters: 
# - instanceNumber
# Return instance host

sub getInstanceStatus 
{
    my $self = shift;
    my $instanceNumber = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getInstanceStatus",1);

    my $ret;

    my $activeinstances = $self->{source}->{runtime}->{activeInstances};

    if (defined($activeinstances) && (scalar @{$activeinstances} gt 0)) {
        for my $inst ( @{$activeinstances} ) {
            if ($inst->{instanceNumber} eq $instanceNumber) {
                $ret = 'up';
                last;
            }
        }
    } else {
        $ret = 'down';
    }

    if (! defined($ret)) {
        $ret = 'down';
    }

    return $ret;
}

# Procedure start
# parameters: none
# Start VDB
# - instance 
# Return job number if job started or undef otherwise

sub start 
{
    my $self = shift;
    my $instance = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::start",1);
    my $operation = "resources/json/delphix/source/" . $self->{source}->{reference} . "/start";
    my $payload;
    if (defined($instance)) {
        my @instarr;
        push(@instarr, $instance + 0);
        my %start_arg = (
            "type" => "OracleStartParameters",
            "instances" => \@instarr
        );
        $payload = encode_json(\%start_arg);
    } else {
        $payload = '{}';
    }
    return $self->runJobOperation($operation,$payload);
}

# Procedure stop
# parameters: none
# Stop VDB
# - instance 
# Return job number if job started or undef otherwise

sub stop 
{
    my $self = shift;
    my $instance = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::stop",1);
    my $operation = "resources/json/delphix/source/" . $self->{source}->{reference} . "/stop";
    my $payload;
    if (defined($instance)) {
        my @instarr;
        push(@instarr, $instance + 0);
        my %start_arg = (
            "type" => "OracleStopParameters",
            "instances" => \@instarr
        );
        $payload = encode_json(\%start_arg);
    } else {
        $payload = '{}';
    }
    return $self->runJobOperation($operation,$payload);
}

# Procedure getBCT
# parameters: none
# Return database bct information

sub getBCT 
{
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getBCT",1);
    my $ret;
    if ($self->getRuntimeStatus() eq 'RUNNING') {
        $ret = $self->{source}->{runtime}->{bctEnabled} ? 'ENABLED' : 'DISABLED';
    } else {
        $ret = 'UNKNOWN';
    }

    return $ret;
}

# Procedure setDefaultParams
# parameters: 
# Get defaults for provisioning
# Return 0 if success, 1 if not found

sub setDefaultParams {
    my $self = shift; 
    my $version = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setDefaultParams",1);
    my $operation = "resources/json/delphix/database/provision/defaults";
 #  {"type":"TimeflowPointTimestamp","timeflow":"ORACLE_TIMEFLOW-306","timestamp":"2016-06-22T12:54:32.000Z"}

    my $ret;

    my $time = $self->{NEWDB}->{timeflowPointParameters};

    my $json_data = to_json($time);

    #print Dumper $json_data;

    #print Dumper $self;

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if (! defined ($result) ) {
        print "There was a problem with setting a default parameters for database " . $self->getName() . ". \n";
        $ret = 1;
    } elsif ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->{"NEWDB"}->{"source"}->{"configParams"} = $result->{result}->{source}->{configParams};
        $ret = 0;
    } else {
        print "There was a problem with setting a default parameters for database " . $self->getName() . ". \n";
        $ret = 1;
    }

    #print Dumper $self->{"NEWDB"};

    return $ret;

}


# Procedure setVersion
# parameters: 
# - version - source version
# Set compatible parameter to first 4 digit of source
# Return 0 if success, 1 if not found

sub setVersion {
    my $self = shift; 
    my $version = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setVersion",1);

    # take only first 4 characters
    my @ver = split ('\.',$version);
    splice (@ver, 4);
    my $ora_ver = join ('.', @ver);
 
    $self->{"NEWDB"}->{"source"}->{"configParams"}->{"compatible"} = $ora_ver;
}

# Procedure setSource
# parameters: 
# - source - source hash
# Set dsource reference by name for new db. 
# Return 0 if success, 1 if not found

sub setSource {
    my $self = shift; 
    my $source = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};

    if (defined ($source)) {

        #print Dumper $source->{container};

        my $sourcetype = $source->{container}->{'type'};
        if (($sourcetype eq 'OracleDatabaseContainer') || ($sourcetype eq 'OracleVirtualSource')) {
            $self->{"NEWDB"}->{"timeflowPointParameters"}->{"container"}  = $source->{container}->{reference};
            return 0;
        } else {
            return 1;
        }
    } else {
        return 1;
    }       

}

# Procedure getListenersNames
# parameters: none
# Return database listeners names

sub getListenersNames
{
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getListenersNames",1);

    my $ret = '';
    
    my $envref;
    
    if (defined($self->{environment})) {
        $envref = $self->{environment}->{reference};
    } 
    
    if (defined($self->{_environment})) {
      if (defined($self->{source}->{nodeListenerList})) {
        my @listarr;
        for my $listref (@{$self->{source}->{nodeListenerList}}) {
          push(@listarr, $self->{_environment}->getListenerName($envref, $listref));
        }
        $ret = join(',', @listarr);
      }
    } else {
        $ret = 'NA';
    }

    return $ret;
}


# Procedure setListener
# parameters: 
# - name - list of listeners names separated by commas
# Set listeners for new db
# Return 0 if success, 1 if not found

sub setListener {
    my $self = shift; 
    my $name = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setListener",1);

    my $environments;
    if (defined($self->{_environment})) {
        $environments = $self->{_environment};
    } else {
        $environments = new Environment_obj($self->{_dlpxObject}, $self->{_debug});
        $self->{_environment} = $environments;
    }

    if (!defined ($self->{'_newenv'})) {
      print "Environment not set\n";
      return 1;
    }

    my @listrefarray;
    
    for my $listname (split(',', $name)) {
      my $listref = $environments->getListenerByName($self->{'_newenv'}, $listname);
      if (defined($listref)) {
        push(@listrefarray, $listref);
      } else {
        print "Listener $listname not found\n.";
        return 1;
      }
    }
        
    $self->{NEWDB}->{source}->{nodeListenerList} = \@listrefarray;
    return 0;
  }

  # Procedure isRAC
  # parameters: 
  # Return 1 if RAC database

  sub isRAC {
      my $self = shift;
      my $ret = 0;
      if (defined($self->{sourceConfig}) && defined($self->{sourceConfig}->{type}) ) {
        if ($self->{sourceConfig}->{type} eq 'OracleRACConfig') {
          $ret = 1
        }
      } 
      return $ret;
  }


# Procedure getUniqueName
# parameters: 
# Get unique name of Oracle database

sub getUniqueName {
    my $self = shift;
    my $ret;
    if (defined($self->{sourceConfig}) && defined($self->{sourceConfig}->{uniqueName}) ) {
      $ret = $self->{sourceConfig}->{uniqueName};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}


# Procedure setName
# parameters: 
# - contname - container name
# - dbname - database name
# - unique_name - database unique name - if not defined set to dbname
# - instance_name - instance name - if not defined set to dbname
# Set name for new db. 

sub setName {
    my $self = shift;
    my $contname = shift;
    my $dbname = shift;
    my $unique_name = shift;
    my $instance_name = shift;
    
    logger($self->{_debug}, "Entering OracleVDB_obj::setName",1);

    if (! defined ($unique_name) ) {
        $unique_name = $dbname;
    }

    if (! defined ($instance_name) ) {
        $instance_name = $dbname;
    }        
    
    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"databaseName"} = $dbname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"uniqueName"} = $unique_name;
    $self->{"NEWDB"}->{"sourceConfig"}->{"instance"}->{"instanceName"} = $instance_name;    
    
}

# Procedure getTemplateRef
# parameters: 
# Return template reference

sub getTemplateRef {
    my $self = shift; 
    logger($self->{_debug}, "Entering OracleVDB_obj::getTemplateRef",1);

    my $ret;
    if (defined($self->{source}->{configTemplate})) {
      $ret = $self->{source}->{configTemplate};
    } 
    return $ret;
}

# Procedure setTemplate
# parameters: 
# - name - template name
# Set template reference by name for new db. 
# Return 0 if success, 1 if not found

sub setTemplate {
    my $self = shift; 
    my $name = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::setTemplate",1);

    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};
    my $templates = new Template_obj($dlpxObject, $debug);

    my $templateitem = $templates->getTemplateByName($name);

    if (defined ($templateitem)) {
        $self->{"NEWDB"}->{"source"}->{"configTemplate"}  = $templateitem;
        delete $self->{"NEWDB"}->{"source"}->{"configParams"};
        return 0;
    } else {
        return 1;
    }       

}

# Procedure getMountPoint
# parameters: 
# Get mountpoint of DB. 

sub getMountPoint {
    my $self = shift; 
    logger($self->{_debug}, "Entering OracleVDB_obj::getMountPoint",1);
    return $self->{"source"}->{"mountBase"};
} 

# Procedure setMountPoint
# parameters: 
# - mountpoint - mount point
# Set mountpoint for new db. 

sub setMountPoint {
    my $self = shift; 
    my $mountpoint = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setMountPoint",1);
    $self->{"NEWDB"}->{"source"}->{"mountBase"} = $mountpoint;
}   

# Procedure setArchivelog
# parameters: 
# - archivelog - type
# Set archivelog or noarchivelog

sub setArchivelog {
    my $self = shift; 
    my $archlog = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setArchivelog",1);

    my $archlog_param = $archlog eq 'yes' ? JSON::true : JSON::false;

    $self->{"NEWDB"}->{"source"}->{"archivelogMode"} = $archlog_param;
}   

# Procedure getArchivelog
# Get archivelog or noarchivelog

sub getArchivelog {
    my $self = shift; 
    logger($self->{_debug}, "Entering OracleVDB_obj::getArchivelog",1);

    my $archlog_param;
    my $archlog;

    if ($self->{_dlpxObject}->getApi() lt "1.5") {
        $archlog = $self->{source}->{runtime}->{archivelogEnabled};
        if ($self->getRuntimeStatus() eq 'RUNNING') {
            $archlog_param = $archlog ? 'archivelog=yes' : 'archivelog=no';
        } else {
            $archlog_param = 'N/A';
        }
    } else {
        $archlog = $self->{source}->{archivelogMode};
        if ($self->getType() eq 'VDB') {
          $archlog_param = $archlog ? 'archivelog=yes' : 'archivelog=no';
        } else {
          if ($self->getRuntimeStatus() eq 'RUNNING') {
              $archlog_param = $archlog ? 'archivelog=yes' : 'archivelog=no';
          } else {
              $archlog_param = 'N/A';
          }
        }
    }

    return $archlog_param;
}   


# Procedure setMapFile
# parameters: 
# - map_file - hash of map file
# Set mountpoint for new db. 

sub setMapFile {
    my $self = shift; 
    my $map_file = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setMapFile",1);
    $self->{"NEWDB"}->{"source"}->{"fileMappingRules"} = $map_file;
}  


# Procedure setNoOpen
# parameters: 
# Set no open database after provision

sub setNoOpen {
    my $self = shift; 
    logger($self->{_debug}, "Entering OracleVDB_obj::setNoOpen",1);
    $self->{"NEWDB"}->{"openResetlogs"} = JSON::false;
}  



# Procedure refresh
# parameters: 
# - timestamp - timestamp / LATEST_POINT / LATEST_SNAPSHOT
# refresh VDB
# Return job number if job started or undef otherwise

sub refresh 
{
    my $self = shift;
    my $timestamp = shift;
    my $changenum = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::refresh",1);
    return $self->VDB_obj::refresh($timestamp,$changenum,'OracleRefreshParameters') ;
}

# Procedure disable
# parameters: 
# - force
# Disable database
# Return job number if job started or undef otherwise

sub disable 
{
    my $self = shift;
    my $force = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::disable",1);
    return $self->VDB_obj::disable($force,'OracleDisableParameters') ;
}

# Procedure delete
# parameters: 
# - force
# Delete database
# Return job number if job started or undef otherwise

sub delete 
{
    my $self = shift;
    my $force = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::delete",1);
    return $self->VDB_obj::delete($force,'OracleDeleteParameters') ;
}


# Procedure rewind
# parameters: 
# - timestamp - timestamp / LATEST_POINT / LATEST_SNAPSHOT
# rewind VDB
# Return job number if job started or undef otherwise

sub rewind 
{
    my $self = shift;
    my $timestamp = shift;
    my $changenum = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::rewind",1);
    return $self->VDB_obj::rewind($timestamp,$changenum,'OracleRollbackParameters') ;
}

# Procedure snapshot
# parameters: 
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot 
{
    my $self = shift;
    my $timestamp = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::snapshot",1);
    my %snapshot_type = (
        "type" => "OracleSyncParameters"
    );
    return $self->VDB_obj::snapshot(\%snapshot_type) ;
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

    logger($self->{_debug}, "Entering OracleVDB_obj::attach_dsource",1);

    my $config = $self->setConfig($source, $source_inst, $source_env);

    if (! defined($config)) {
        print "Source database $source not found\n";
        return undef;
    }

    if ($self->{_sourceconfig}->validateDBCredentials($config->{reference}, $dbuser, $password)) {
        print "Username or password is invalid.\n";
        return undef;
    }


    my $source_env_ref = $self->{_repository}->getEnvironment($config->{repository});

    my $source_os_ref = $self->{_environment}->getEnvironmentUserByName($source_env_ref,$source_osuser);

    if (!defined($source_os_ref)) {
        print "Source OS user $source_osuser not found\n";
        return undef;
    }

    my @empty;

    my %operations = (
        "type" => "LinkedSourceOperations",
        "configureClone" => \@empty,
        "postRefresh" => \@empty,
        "preRefresh" => \@empty
    );

    my %attach_data = (
        "type" => "OracleAttachSourceParameters",
        "source" =>  {
            "type" => "OracleLinkedSource",
            "config" => $config->{reference},
            "operations" => \%operations
        },
        "dbCredentials" => {
            "type" => "PasswordCredential",
            "password" => $password
        },
        "dbUser" => $dbuser,
        "environmentUser" => $source_os_ref
    );

    my $operation = 'resources/json/delphix/database/'. $self->{container}->{reference} .'/attachSource' ;
    my $json_data = encode_json(\%attach_data);
    return $self->runJobOperation($operation,$json_data, 'ACTION');    
}


# Procedure setRacProvisioning
# parameters: 
# - instances - array of hashes ( instance no - instance name - node )


sub setRacProvisioning {
    my $self = shift; 

    my $instances = shift;

    my @instanceArray;


    my $environments = new Environment_obj($self->{_dlpxObject}, $self->{_debug});

    my $env_nodes = $environments->getOracleClusterNode($self->{'_newenv'});

    my $instance_base = $self->{"NEWDB"}->{"sourceConfig"}->{"instance"}->{"instanceName"};

    my $instance_number = 1;

    if (defined ($instances) ) {
        # provision for a list 
        my %node_names = map { $_->{name} => $_->{reference} } @{$env_nodes};

        my %instance_numbers;

        for my $inst (@{$instances}) {
            my $nodename = (split(',',$inst))[0];
            if (defined($node_names{$nodename})) {

                my $inst_no = (split(',',$inst))[2] + 0;

                if (defined ($instance_numbers{$inst_no})) {
                    print "Instance number " . $inst_no . " has to be unique.\n";
                    return 1;   
                }

                $instance_numbers{$inst_no} = 1;

                my %inst = (
                    "type" => "OracleRACInstance",
                    "instanceNumber" => $inst_no + 0,
                    "instanceName" =>  (split(',',$inst))[1],
                    "node" => $node_names{$nodename}
                );
                push (@instanceArray, \%inst);    
            } else {
                print "Node name " . $nodename . " not found.\n";
                return 1;
            }
        }

    } else {
        # provision for all nodes
        for my $cluster_node (@{$env_nodes}) {
            my %inst = (
                "type" => "OracleRACInstance",
                "instanceNumber" => $instance_number + 0,
                "instanceName" =>  $instance_base . $instance_number,
                "node" => $cluster_node->{reference}
            );
            $instance_number = $instance_number + 1;
            push (@instanceArray, \%inst);
        }

    }   



    $self->{"NEWDB"}->{"sourceConfig"}->{"type"} = 'OracleRACConfig';
    delete $self->{"NEWDB"}->{"sourceConfig"}->{"instance"};
    $self->{"NEWDB"}->{"sourceConfig"}->{"instances"} = \@instanceArray;



    return 0;

}

# Procedure addSource
# parameters: 
# - source - name of source DB
# - source_osuser - name of source OS user
# - dbuser - DB user name
# - password - DB user password
# - dsource_name - name of dsource in environment
# - group - dsource  group

# Start job to add Oracle dSource 
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


    logger($self->{_debug}, "Entering OracleVDB_obj::addSource",1);

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


    my $source_env_ref = $self->{_repository}->getEnvironment($config->{repository});

    my $source_os_ref = $self->{_environment}->getEnvironmentUserByName($source_env_ref,$source_osuser);

    if (!defined($source_os_ref)) {
        print "Source OS user $source_osuser not found\n";
        return undef;
    }


    my $logsync_param = $logsync eq 'yes' ? JSON::true : JSON::false;

    my %dsource_params = (
          "environmentUser" => $source_os_ref,
          "source" => {
            "type" => "OracleLinkedSource",
            "bandwidthLimit" => 0,
            "filesPerSet" => 5,
            "rmanChannels" => 2,
            "operations" => {
              "type" => "LinkedSourceOperations",
              "preSync" => [],
              "postSync" => []
            },
            "config" => $config->{reference},
          },
          "type" => "OracleLinkParameters",
          "container" => {
            "type" => "OracleDatabaseContainer",
            "sourcingPolicy" => {
              "logsyncEnabled" => $logsync_param,
              "type" => "OracleSourcingPolicy",
              "logsyncMode" => "ARCHIVE_REDO_MODE"
            },
            "name" => $dsource_name,
            "group" => $self->{"NEWDB"}->{"container"}->{"group"}
          },
          "dbCredentials" => {
            "type" => "PasswordCredential",
            "password" => $password
          },
          "linkNow" => JSON::true,
          "dbUser" => $dbuser
    );


    my $operation = 'resources/json/delphix/database/link';
    my $json_data = to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    
    logger($self->{_debug}, $json_data, 1);

    return $self->runJobOperation($operation,$json_data, 'ACTION');

}


# Procedure createVDB
# parameters: 
# - group - new DB group
# - env - new DB environment
# - home - new DB home
# - rac
# - instance array
# Start job to create Single Instance Oracle VDB
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef 

sub createVDB {
    my $self = shift; 

    my $group = shift;
    my $env = shift;
    my $home = shift;
    my $instances = shift;


    logger($self->{_debug}, "Entering OracleVDB_obj::createVDB",1);

    if ( $self->setGroup($group) ) {
        print "Group $group not found. VDB won't be created\n";
        return undef;
    }

    # if ( $self->setEnvironment($env) ) {
    #     print "Environment $env not found. VDB won't be created\n";
    #     return undef;
    # }

    if ( $self->setHome($home) ) {
        print "Home $home in environment $env not found. VDB won't be created\n";
        return undef;
    }

    if ( ! defined($self->{"NEWDB"}->{"container"}->{"name"} ) ) {
        print "Set name using setName procedure before calling create VDB. VDB won't be created\n";
        return undef;
    }


    if ($self->{'_newenvtype'} eq 'OracleCluster') {
        if ( $self->setRacProvisioning($instances) ) {
            print "Problem with node names or instance numbers. Please double check.";
            return undef;
        }
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

sub v2pSI {
    my $self = shift; 

    my $env = shift;
    my $home = shift;


    logger($self->{_debug}, "Entering OracleVDB_obj::v2pSI",1);

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

    $self->{"NEWDB"}->{"type"} = "OracleExportParameters";
    $self->{"NEWDB"}->{"sourceConfig"}->{"linkingEnabled"} = JSON::true;


    delete $self->{"NEWDB"}->{"container"};
    delete $self->{"NEWDB"}->{"source"};
    my $operation = 'resources/json/delphix/database/export';
    my $json_data = $self->getJSON();
    return $self->runJobOperation($operation,$json_data);

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


    logger($self->{_debug}, "Entering OracleVDB_obj::upgradeVDB",1);
    return $self->VDB_obj::upgradeVDB($home,'OracleSIConfig') ;

}


#######################
# end of OracleVDB_obj class
#######################

# class MSSQLVDB_obj - is a child class of VDB_obj

package MSSQLVDB_obj;
use Data::Dumper;
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
      if ( (uc $validatedSyncMode eq 'FULL_OR_DIFFERENTIAL' ) || (uc $validatedSyncMode eq 'FULL' ) || (uc $validatedSyncMode eq 'TRANSACTION_LOG' ) ) 
      {
        $vsm = $validatedSyncMode;
      } else {
        print "Invalid validatedSyncMode option - $validatedSyncMode \n";
        return undef;
      }
                                  
    }



    my %dsource_params;

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
    }

    if (defined($dumppwd)) {
      $dsource_params{source}{encryptionKey} = JSON::true;
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
    return $self->{source}->{validatedSyncMode};

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

    my %attach_data = (
        "type" => "MSSqlAttachSourceParameters",
        "source" =>  {
            "type" => "MSSqlLinkedSource",
            "config" => $config->{reference},
            "operations" => \%operations
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


########################

package SybaseVDB_obj;
use Data::Dumper;
use JSON;    
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

    my %dsource_params = (
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

    my $operation = 'resources/json/delphix/database/link';
    my $json_data =to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    logger($self->{_debug}, $json_data, 1);
    # there is couple of jobs - we need to monitor action
    return $self->runJobOperation($operation,$json_data, 'ACTION');

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

    my %dsource_params = (
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
        "stagingRepository"=> $stagingrepo,
    );




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
# Run snapshot
# Return job number if job started or undef otherwise

sub snapshot 
{
    my $self = shift;
    my $frombackup = shift;
    logger($self->{_debug}, "Entering SybaseVDB_obj::snapshot",1);

    if (! defined ($frombackup) ) {
        return undef;
    };

    my %snapshot_type;

    if ( $frombackup eq "yes" ) {
        %snapshot_type = (
            "type" => "ASELatestBackupSyncParameters"
        );
    } else {
        %snapshot_type = (
            "type" => "ASENewBackupSyncParameters"
        );
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

########################

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

    delete $self->{"NEWDB"}->{"sourceConfig"}->{"linkingEnabled"};

    $self->setPort($port);
    $self->setMountPoint($mountpoint);

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

package AppDataVDB_obj;
use Data::Dumper;
use JSON;    
use Toolkit_helpers qw (logger);
our @ISA = qw(VDB_obj);

sub new {
    my $class  = shift;
    my $dlpxObject = shift;
    my $debug = shift;    
    logger($debug, "Entering AppDataVDB_obj::constructor",1);
    # call VDB_obj constructor
    my $self       = $class->SUPER::new($dlpxObject, $debug); 

   # MySQL specific properties 


    my @configureClone;
    my @postRefresh;
    my @preRefresh;
    my @configParams;
    my @mntPoints;

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
                    "repository" => ""
                    #"parameters" => {}
                    #"parameters" => \@configParams,
            },
            "source" => {
                    "type" => "AppDataVirtualSource",
                    "additionalMountPoints" => \@mntPoints,
                    "operations" => \%operations
                    #"parameters" => {}
                    #"parameters" => \@configParams,
            },
            "timeflowPointParameters" => {
                "type" => "TimeflowPointSemantic",
                "container" => "",
                "location" => "LATEST_SNAPSHOT"
            },
    );

    $self->{"NEWDB"} = \%prov; 
    $self->{_dbtype} = 'vFiles';

    if ($self->{_dlpxObject}->getApi() gt "1.6") {
        $prov{"source"}{"parameters"} = {};
        $prov{"sourceConfig"}{"parameters"} = {};
    }
            
    return $self;
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
    logger($self->{_debug}, "Entering AppDataVDB_obj::snapshot",1);

    if (! defined ($frombackup) ) {
        return undef;
    };

    my %snapshot_type;

    %snapshot_type = (
            "type" => "AppDataSyncParameters",
            "resync" => JSON::false
    );
    
    return $self->VDB_obj::snapshot(\%snapshot_type) ;
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

    logger($self->{_debug}, "Entering AppDataVDB_obj::createVDB",1);


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
    my $json_data = $self->getJSON();

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
    logger($self->{_debug}, "Entering AppDataVDB_obj::setName",1);
    
    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"source"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"name"} = $dbname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"path"} = $dbname;
    
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
    logger($self->{_debug}, "Entering AppDataVDB_obj::setSource",1);

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