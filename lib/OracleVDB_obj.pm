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
# - OracleVDB_obj - Oracle VDB
#
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#

# class OracleVDB_obj - is a child class of VDB_obj

package OracleVDB_obj;
use strict;
use warnings;
use Data::Dumper;
use Template_obj;
use JSON;
use Toolkit_helpers qw (logger);
use SourceConfig_obj;
our @ISA = qw(VDB_obj);

# use Group_obj;
# use Host_obj;
# use Source_obj;
# use Snapshot_obj;
# use Action_obj;
# use Namespace_obj;
# use Bookmark_obj;
# use SourceConfig_obj;
use Environment_obj;
use Repository_obj;
# use Toolkit_helpers qw (logger);
# use Hook_obj;

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


# Procedure getConfig
# parameters: none
# Return database config

sub getConfig 
{
    my $self = shift;
    my $templates = shift;
    my $backup = shift;
    
    logger($self->{_debug}, "Entering OracleVDB_obj::getConfig",1);
    
    my $config = '';
    my $joinsep;
    
    if (defined($backup)) {
      $joinsep = ' ';
    } else {
      $joinsep = ',';
    }

    
    if ($self->getType() eq 'VDB') {
    
      my $mntpoint = $self->getMountPoint();
      my $archlog = $self->getArchivelog();
      my $tempref = $self->getTemplateRef();
      my $listnames = $self->getListenersNames();
      my $redogroups = $self->getRedoGroupNumber();

        

      
      my $cdbname = $self->getCDBContainer();
      
      if (defined($cdbname)) {
        #vPDB
        $config = join($joinsep,($config, "-cdb $cdbname"));
      } else {
        #non vPDB
        if ($redogroups ne 'N/A') {
          $config = join($joinsep,($config, "-redoGroup $redogroups")); 
          my $redosize = $self->getRedoGroupSize();
          if (($redosize ne 'N/A') && ($redosize ne 0)) {
            $config = join($joinsep,($config, "-redoSize $redosize"));
          }
        }
        $config = join($joinsep,($config, "-$archlog")) ;   
        if (defined($listnames) && ($listnames ne '')) {
          $config = join($joinsep,($config, "-listeners $listnames"));
        }     
      }

                  
      if (defined($tempref)) {
        my $tempname = $templates->getTemplate($tempref)->{name};
        $config = join($joinsep,($config, "-template $tempname"));
      }
      $config = join($joinsep,($config, "-mntpoint \"$mntpoint\""));
        
      #if one instance use -instanceName
      my $instances = $self->getInstances();
                          
      if ($self->isRAC()) {
        #rac 
        my $rac = '';
        for my $inst (@{$instances}) {
          $rac = $rac . "-rac_instance " . $self->getInstanceNode($inst->{instanceNumber}) . "," . $inst->{instanceName} . "," . $inst->{instanceNumber} . " "; 
        }
        $config = join($joinsep,($config, $rac));
      } else {
        if ($instances ne 'UNKNOWN') {
          $config = join($joinsep,($config, "-instname " . $instances->[-1]->{instanceName}));
        }
      }
        
      my $unique = $self->getUniqueName();
      if ($unique ne 'N/A') {
        $config = join($joinsep,($config, "-uniqname $unique"));
      }
    
    }
    
    if ( (my $rest) = $config =~ m/^,(.*)/ ) {
      $config = $rest;
    }
  
    return $config;
    
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

# Procedure getInstanceNumber
# parameters: 
# - instance name
# Return instance number or undef if not found

sub getInstanceNumber 
{
    my $self = shift;
    my $instancename = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getInstanceNumber",1);

    my $ret;
    
    my $inst = $self->{instances};

    my @num = grep { $inst->{$_}->{name} eq $instancename } keys %{$inst};

    if (scalar(@num) ne 1) {
      $ret = undef;
    } else {
      $ret = $num[-1]+0;
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

# Procedure getCDBContainer
# parameters: 
# Return CDB name or undef if not vPDB

sub getCDBContainer 
{
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getCDBContainer",1);

    my $ret;

    if ($self->{sourceConfig}->{type} eq 'OraclePDBConfig') {
      my $cdbref = $self->{sourceConfig}->{cdbConfig};      
      $ret = $self->{_sourceconfig}->getName($cdbref); 
    };
    
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
# Set dsource reference by name for new db
# Return 0 if success, 1 if not found

sub setSource {
    my $self = shift; 
    my $source = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};
      
    $self->{_source} = $source;

    if (defined ($source)) {
        
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
    if (defined($self->{sourceConfig}) && ($self->{sourceConfig} ne 'NA') && defined($self->{sourceConfig}->{uniqueName}) ) {
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


# Procedure getRedoGroupNumber
# Get redo groups number of VDB

sub getRedoGroupNumber {
    my $self = shift; 
    logger($self->{_debug}, "Entering OracleVDB_obj::getRedoGroupNumber",1);

    my $redogroups = defined($self->{source}->{redoLogGroups}) ? $self->{source}->{redoLogGroups} : 'N/A';

    return $redogroups;
} 

# Procedure setRedoGroupNumber
# Set redo groups number of VDB

sub setRedoGroupNumber {
    my $self = shift; 
    my $redogroups = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setRedoGroupNumber",1);

    if ($self->{_dlpxObject}->getApi() ge "1.5") {
      $self->{NEWDB}->{source}->{redoLogGroups} = 0 + $redogroups;
    }
    
} 

# Procedure getRedoGroupSize
# Get redo groups size in MB of VDB

sub getRedoGroupSize {
    my $self = shift; 
    logger($self->{_debug}, "Entering OracleVDB_obj::getRedoGroupSize",1);

    my $redogroups = defined($self->{source}->{redoLogSizeInMB}) ? $self->{source}->{redoLogSizeInMB} : 'N/A';

    return $redogroups;
} 

# Procedure setRedoGroupNumber
# Set redo groups size in MB of VDB

sub setRedoGroupSize {
    my $self = shift; 
    my $redosize = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setRedoGroupSize",1);

    if ($self->{_dlpxObject}->getApi() ge "1.5") {
      $self->{NEWDB}->{source}->{redoLogSizeInMB} = 0+ $redosize;
    }
    
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

    my %attach_data;
    if ($self->{_dlpxObject}->getApi() lt "1.8") {
      %attach_data = (
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
    } else {
      %attach_data = (
          "type" => "AttachSourceParameters",
          "attachData" => {
                "type" => "OracleAttachData",
                "config" => $config->{reference},
                "dbCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
                },
                "dbUser" => $dbuser,
                "environmentUser" => $source_os_ref
          }
      );    
    }

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

# Procedure discoverPDB
# parameters: 
# - cdb - name of source CDB
# - source_inst - name of source inst
# - source_env - name of source env
# - cdbuser - db user in CDB
# - cdbpass - db pass in CDB


# Discover PDB in a specified CDB

sub discoverPDB {
    my $self = shift; 
    my $source_inst = shift;
    my $source_env = shift;
    my $cdbname = shift;
    my $cdbuser = shift;
    my $cdbpass = shift;
    my $ret;
    logger($self->{_debug}, "Entering OracleVDB_obj::discoverPDB",1);

    my $cdb = $self->setConfig($cdbname, $source_inst, $source_env);

    if (! defined($cdb)) {
        print "Source container database $cdbname not found\n";
        return undef;
    }
    
    if ($cdb->{'cdbType'} ne 'ROOT_CDB') {
      my %updatecdb = (
        "type" => "OracleSIConfig",
        "user" => $cdbuser,
        "credentials" => {
          "type" => "PasswordCredential",
          "password" => $cdbpass
        }
      );
      
      my $json_data = encode_json(\%updatecdb);

      logger($self->{_debug}, $json_data, 2);

      my $operation = 'resources/json/delphix/sourceconfig/' . $cdb->{reference};

      my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

      if (! defined ($result) ) {
          print "There was a problem with discovering PDB database $cdbname \n";
          return 1;
      } elsif ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
          print "Setting credential for CDB $cdbname sucessful.\n";
      } else {
          print "There was a problem with setting a credentials for CDB $cdbname \n";
          return 1;
      }  
      
      $self->{_sourceconfig}->refresh();
      if ($self->{_sourceconfig}->getSourceConfig($cdb->{reference})->{'cdbType'} eq 'ROOT_CDB') {
        return 0;
      } else {
        print "Database $cdbname is not a CDB \n";
        return 1;    
      }
    } else {
      return 0;
    }

}

# Procedure addSource
# parameters: 
# - source - name of source DB
# - source_inst - name of source inst
# - source_env - name of source env
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
        print "Username or password is invalid or database is down.\n";
        return undef;
    }
    
    if ( $self->setGroup($group) ) {
        print "Group $group not found. dSource won't be created\n";
        return undef;
    }

    if (!defined($self->{_repository})) { 
        $self->{_repository} = new Repository_obj($self->{_dlpxObject}, $self->{_debug});
    }

    if (!defined($self->{_environment})) {
        $self->{_environment} = new Environment_obj($self->{_dlpxObject}, $self->{_debug});
    }

    my $source_env_ref = $self->{_repository}->getEnvironment($config->{repository});

    my $source_os_ref = $self->{_environment}->getEnvironmentUserByName($source_env_ref,$source_osuser);

    if (!defined($source_os_ref)) {
        print "Source OS user $source_osuser not found\n";
        return undef;
    }


    my $logsync_param = $logsync eq 'yes' ? JSON::true : JSON::false;

    my %dsource_params;
    
    if ($self->{_dlpxObject}->getApi() lt "1.8") {
      %dsource_params = (
            "environmentUser" => $source_os_ref,
            "source" => {
              "type" => "OracleLinkedSource",
              "bandwidthLimit" => 0,
              "filesPerSet" => 5,
              "rmanChannels" => 2,
              "compressedLinkingEnabled" => JSON::true,
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
      
      if ($config->{type} eq 'OraclePDBConfig') {
        $dsource_params{"type"} = 'OraclePDBLinkParameters';
      } 
      
    } else {
        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "OracleLinkData",
              "config" => $config->{reference},
              "sourcingPolicy" => {
                  "type" => "OracleSourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "dbCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
              },
              "dbUser" => $dbuser,
              "environmentUser" => $source_os_ref,
              "linkNow" => JSON::true,
              "compressedLinkingEnabled" => JSON::true
          }
      );
      
      if ($config->{type} eq 'OraclePDBConfig') {
        $dsource_params{"linkData"}{"type"} = "OraclePDBLinkData";
      } 
      
    }


    my $operation = 'resources/json/delphix/database/link';
    my $json_data = to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    
    logger($self->{_debug}, $json_data, 1);

    return $self->runJobOperation($operation,$json_data, 'ACTION');

}

# Procedure findCDBonEnvironment
# parameters: 
# - group - new DB group
# - env - new DB environment
# - home - new DB home
# - rac
# - instance array
# Start job to create Single Instance Oracle VDB
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef 

sub findCDBonEnvironment {
    my $self = shift; 
    my $cdbname = shift;

    my $sourceconfig = new SourceConfig_obj($self->{_dlpxObject}, $self->{_debug});
    
    my $cdbconf;
    
    if (defined($cdbname)) {
      my $cdbobj = $sourceconfig->getSourceConfigByName($cdbname);
      if (defined($cdbobj)) {
        $cdbconf = $cdbobj->{reference};   
      } else {
        print "CDB named $cdbname not found in Oracle Home and envitonment\n";
      }
    } else {
      my $list = $sourceconfig->getSourceConfigsListForRepo($self->{"NEWDB"}->{"sourceConfig"}->{"repository"});
      my @cdbList = grep { defined($sourceconfig->getSourceConfig($_)->{cdbType}) && (($sourceconfig->getSourceConfig($_))->{cdbType} eq 'ROOT_CDB') } @{$list};

      if (scalar(@cdbList) > 1) {
        print "There is more than 1 CDB in Oracle home. Please specify it\n";
        return undef;
      } elsif (scalar(@cdbList) < 1) {
        print "There is non CDB in found in Oracle Home and environment. Please check it\n";
        return undef;
      } 
      
      $cdbconf = $cdbList[-1];
      
    }

    
    return $cdbconf;
    
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
    my $cdbname = shift;


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
  
  
    logger($self->{_debug}, "Target environment type " . Dumper $self->{_newenvtype}, 2 );
    
    if ($self->{'_newenvtype'} eq 'OracleCluster') {
        if ( $self->setRacProvisioning($instances) ) {
            print "Problem with node names or instance numbers. Please double check.";
            return undef;
        }
    } else {
      my $configtype = $self->{_source}->getSourceConfigType();
      if ($configtype eq 'OracleRACConfig') {
        $configtype = "OracleSIConfig";
      }
      $self->{"NEWDB"}->{"sourceConfig"}->{"type"} = $configtype;
         
    }

    logger($self->{_debug}, "Target sourceConfig type " . Dumper $self->{"NEWDB"}->{"sourceConfig"}->{"type"}, 2 );

    if ( $self->{"NEWDB"}->{"sourceConfig"}->{"type"} eq 'OraclePDBConfig') {
      if (!defined($cdbname)) {
        print "Container name (-cdb) for vPDB provisioning has to be set. VDB won't be created\n";
        return undef;
      }

      my $cdbconf = $self->findCDBonEnvironment($cdbname);  
      if (!defined($cdbconf)) {
        print "Container name $cdbname not found. VDB won't be created\n";
        return undef;
      }   
      $self->{"NEWDB"}->{"sourceConfig"}->{"cdbConfig"} = $cdbconf;
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

1;
