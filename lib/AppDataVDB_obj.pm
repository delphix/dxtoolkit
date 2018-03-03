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
# - AppDataVDB_obj - vFiles VDB
#
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#

# class AppDataVDB_obj - is a child class of VDB_obj

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

    logger($self->{_debug}, "Entering AppDataVDB_obj::getdSourceBackup",1);

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
    logger($self->{_debug}, "Entering AppDataVDB_obj::getVDBBackup",1);

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
    
    logger($self->{_debug}, "Entering AppDataVDB_obj::getConfig",1);
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

# Procedure setEmpty
# parameters: 
# set a flag to create a empty vFiles

sub setEmpty {
    my $self = shift; 

    my $group = shift;
    my $env = shift;
    my $inst = shift;
    my $mountpoint = shift;

    logger($self->{_debug}, "Entering AppDataVDB_obj::setEmpty",1);
    $self->{_empty} = 1;
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

    if (defined($self->{_empty})) {
      delete $self->{NEWDB}->{timeflowPointParameters};
      $self->{NEWDB}->{type} = "AppDataEmptyVFilesCreationParameters";
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
# Start job to add AppData dSource 
# all above parameters are required. Additional parameters should by set by setXXXX procedures before this one is called
# Return job number if provisioning has been started, otherwise return undef 

sub addSource {
    my $self = shift; 
    my $source = shift;
    my $source_inst = shift;
    my $source_env = shift;
    my $source_osuser = shift;
    my $dsource_name = shift;
    my $group = shift;
    

    logger($self->{_debug}, "Entering AppDataVDB_obj::addSource",1);

    my $config = $self->setConfig($source, $source_inst, $source_env);

    if (! defined($config)) {
        print "Source database $source not found\n";
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
    
    my @followarray;
    my @excludes;
    my %dsource_params;
    
    if ($self->{_dlpxObject}->getApi() lt "1.8") {
  
      my %dsource_params = (
          "type" => "AppDataLinkParameters",
          "container" => {
              "type" => "AppDataContainer",
              "name" => $dsource_name,
              "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          },
          "source" => {
              "type" => "AppDataLinkedDirectSource",
              "config" => $config->{reference},
              "excludes" => \@excludes,
              "followSymlinks" => \@followarray
          },
          "environmentUser" => $source_os_ref
      );
      
      if ($self->{_dlpxObject}->getApi() gt "1.6") {
          $dsource_params{"source"}{"parameters"} = {};
      }
    } else {
            
      %dsource_params = (
        "type" => "LinkParameters",
        "group" => $self->{"NEWDB"}->{"container"}->{"group"},
        "name" => $dsource_name,
        "linkData" => {
            "type" => "AppDataDirectLinkData",
            "config" => $config->{reference},
            "environmentUser" => $source_os_ref,
            "excludes" => \@excludes,
            "followSymlinks" => \@followarray,
            "parameters" => {}
        }
      );
    }

    my $operation = 'resources/json/delphix/database/link';
    my $json_data =to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);
    logger($self->{_debug}, $json_data, 1);
    # there is couple of jobs - we need to monitor action
    return $self->runJobOperation($operation,$json_data, 'ACTION');

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

# Procedure getAdditionalMountpoints 
# parameters: none
# Return an array with combained list of additional mount point env,path,sharedPath

sub getAdditionalMountpoints 
{
    my $self = shift;
    logger($self->{_debug}, "Entering AppDataVDB_obj::getAdditionalMountpoints",1);
    
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
    logger($self->{_debug}, "Entering AppDataVDB_obj::setAdditionalMountpoints",1);
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
        'type' => 'AppDataAdditionalMountPoint',
        'mountPath' => $path,
        'sharedPath' => $shared,
        'environment' => $env_obj->{reference}
      );
      
      push (@additionalMountPoints, \%addmount_hash);
      
    }
    
    $self->{NEWDB}->{source}->{additionalMountPoints} = \@additionalMountPoints;
    return 0;
}


# Procedure getDatabaseName
# parameters: none
# Return database name

sub getDatabaseName 
{
    my $self = shift;
    logger($self->{_debug}, "Entering AppDataVDB_obj::getDatabaseName",1);
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

1;
