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
use version;
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
    my $groups = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::getConfig",1);

    my $config = '';
    my $joinsep;
    my $vcdb;

    if (!defined($self->{_databases}->{_vcdblist})) {
      my %vcdblist;
      $self->{_databases}->{_vcdblist} = \%vcdblist;
    }


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


      my $cdbref = $self->getCDBContainerRef();

      if (defined($cdbref)) {
        #vPDB

        #check if vCDB

        my $sourceobj = $self->{_source}->getSourceByConfig($cdbref);

        if (($sourceobj->{type} eq 'OracleVirtualSource') && (! defined($self->{_databases}->{_vcdblist}->{$cdbref}))) {
            # this is a vCDB
            # it is first time we see it 
            $self->{_databases}->{_vcdblist}->{$cdbref} = 1;

            if (defined($sourceobj->{configTemplate})) {
              my $vcdbtempname = $templates->getTemplate($sourceobj->{configTemplate})->{name};
              $config = join($joinsep,($config, "-vcdbtemplate \"$vcdbtempname\""));
            }


            my $dbobj = $self->{_databases}->getDB($sourceobj->{container});

            if (defined($dbobj)) {
              my $vcdbdbname = $dbobj->getDatabaseName();
              my $vcdbuniqname = $dbobj->getUniqueName();
              my $instances = $dbobj->getInstances();
              my $vcdbinstname = $instances->[-1]->{instanceName};
              my $vcdbname = $dbobj->getName();
              my $vcdbgroupname = $groups->getName($dbobj->getGroup());

              if ($dbobj->isRAC()) {
                #rac
                my $rac = '';
                for my $inst (@{$instances}) {
                  $rac = $rac . "-vcdbrac_instance " . $dbobj->getInstanceNode($inst->{instanceNumber}) . "," . $inst->{instanceName} . "," . $inst->{instanceNumber} . " ";
                }
                $config = join($joinsep,($config, $rac));
              } else {
                if ($instances ne 'UNKNOWN') {
                  $config = join($joinsep,($config, "-vcdbinstname " . $instances->[-1]->{instanceName}));
                }
              }

              $vcdb = 1;

              if (defined($dbobj->{sourceConfig}->{"tdeKeystorePassword"})) {
                $config = join($joinsep,($config, "-vdbtdepassword xxxxxxxx"));
              }


              if (defined($self->{"source"}->{"targetVcdbTdeKeystorePath"})) {
                $config = join($joinsep,($config, "-vcdbtdekeystore " . $self->{"source"}->{"targetVcdbTdeKeystorePath"}));
              }


              $config = join($joinsep,($config, "-vcdbname $vcdbname -vcdbdbname $vcdbdbname -vcdbuniqname $vcdbuniqname -vcdbgroup \"$vcdbgroupname\""));
            } else {
              print "Something went wrong. No vCDB found.\n";
              $config = join($joinsep,($config, "vCDB parameters not found"));
            }

        } else {
            # this is a CDB
            my $cdbname = $self->{_sourceconfig}->getName($cdbref);
            $config = join($joinsep,($config, "-cdb $cdbname"));
        }
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
        $config = join($joinsep,($config, "-template \"$tempname\""));
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

      my $cust = $self->getCustomEnv($joinsep);
      if ($cust ne '') {
        $config = join($joinsep,($config, $cust));
      }

      my $tde = $self->getTDE($joinsep, $vcdb);
      if ($tde ne '') {
        $config = join($joinsep,($config, $tde));
      }


    } else {
      # dSource config for Oracle

      my $logsyncmode = $self->getLogSyncMode();
      if ($logsyncmode ne 'unknown') {
        $config = join($joinsep,($config, "-logsyncmode $logsyncmode"));
      }

      my $cdbref = $self->getCDBContainerRef();
      if (defined($cdbref)) {

        my $cdbuser = $self->{_sourceconfig}->getDBUser($cdbref);
        my $cdbname = $self->{_sourceconfig}->getName($cdbref);

        if (defined($cdbuser)) {
          $config = join($joinsep,($config, "-cdbuser \"$cdbuser\""));
          $config = join($joinsep,($config, "-cdbpass xxxxxxxx"));
          $config = join($joinsep,($config, "-cdbcont $cdbname"));
        }



      }


    }

    if ( (my $rest) = $config =~ m/^,(.*)/ ) {
      $config = $rest;
    }

    return $config;

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
            if ($self->{container}->{contentType} eq 'ROOT_CDB') {
              return "CDB"
            } else {
              return "dSource";
            }
        } else {
            if ($self->{container}->{contentType} eq 'ROOT_CDB') {
              return "vCDB";
            } else {
              return "VDB";
            }
        }
    } else {
        return "detached";
    }
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

# Procedure getCDBContainerRef
# parameters:
# Return CDB ref or undef if not vPDB

sub getCDBContainerRef
{
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getCDBContainerRef",1);

    my $ret;

    if ($self->{sourceConfig} ne 'NA') {
      if ($self->{sourceConfig}->{type} eq 'OraclePDBConfig') {
        my $cdbref = $self->{sourceConfig}->{cdbConfig};
        $ret = $cdbref;
      };
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
# Set dsource reference by name for new db
# Return 0 if success, 1 if not found

sub setSource {
    my $self = shift;
    my $source = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setSource",1);

    my $dlpxObject = $self->{_dlpxObject};

    $self->{_sourcedb} = $source;

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

    my $listloc;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
      $listloc = 'nodeListenerList';
    } else {
      $listloc = 'nodeListeners';
    }


    if (defined($self->{_environment})) {
      if (defined($self->{source}->{$listloc})) {
        my @listarr;
        for my $listref (@{$self->{source}->{$listloc}}) {
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


    if (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.8.0)) {
      $self->{NEWDB}->{source}->{nodeListenerList} = \@listrefarray;
    } else {
      $self->{NEWDB}->{source}->{nodeListeners} = \@listrefarray;
    }
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

    if ($self->{_sourcedb}->{container}->{contentType} eq "NON_CDB") {
      if (length($dbname) > 8) {
        print "Max. size of DB_NAME for Oracle is 8 characters\n.";
        print "VDB won't be created\n";
        return 1
      }
    } else {
      if (length($dbname) > 30) {
        print "Max. size of PDB name for Oracle is 30 characters\n.";
        print "VDB won't be created\n";
        return 1
      }
    }

    $self->{"NEWDB"}->{"container"}->{"name"} = $contname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"databaseName"} = $dbname;
    $self->{"NEWDB"}->{"sourceConfig"}->{"uniqueName"} = $unique_name;
    $self->{"NEWDB"}->{"sourceConfig"}->{"instance"}->{"instanceName"} = $instance_name;
    return 0;
}

# Procedure getRuntimeStatus
# parameters: none
# Return database runtime status

sub getRuntimeStatus
{
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getRuntimeStatus",1);

    my $ret;

    my $cdbref = $self->getCDBContainerRef();

    if (defined($cdbref)) {
      #vPDB - API is showing uknown for PDB status if vCDB is used
      # this is a workaround for it

      my $sourceobj = $self->{_source}->getSourceByConfig($cdbref);

      if (defined($sourceobj) && ($sourceobj->{type} eq 'OracleVirtualSource')) {
        if (defined($sourceobj) && defined($sourceobj->{runtime})) {
            $ret = $sourceobj->{runtime}->{status};
        } else {
            $ret = 'NA';
        }
      } else {
        if (defined($self->{source}->{runtime})) {
            $ret = $self->{source}->{runtime}->{status};
        } else {
            $ret = 'NA';
        }
      }



    } else {
      if (defined($self->{source}->{runtime})) {
          $ret = $self->{source}->{runtime}->{status};
      } else {
          $ret = 'NA';
      }
    }



    return $ret;
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

# Procedure getTemplate
# parameters:
# - name - template name
# Return template ref

sub getTemplate {
  my $self = shift;
  my $name = shift;

  logger($self->{_debug}, "Entering OracleVDB_obj::getTemplate",1);

  my $dlpxObject = $self->{_dlpxObject};
  my $debug = $self->{_debug};
  my $templates;


  if (defined($self->{_templates})) {
    $templates = $self->{_templates};
  } else {
    $templates = new Template_obj($dlpxObject, $debug);
    $self->{_templates} = $templates;
  }

  my $templateitem = $templates->getTemplateByName($name);

  return $templateitem;

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

    my $templateitem = $self->getTemplate($name);

    if (defined ($templateitem)) {
        $self->{"NEWDB"}->{"source"}->{"configTemplate"}  = $templateitem;
        delete $self->{"NEWDB"}->{"source"}->{"configParams"};
        return 0;
    } else {
        return 1;
    }
}


# Procedure setTemplateV2P
# parameters:
# - name - template name
# Set template reference by name for v2p db.
# Return 0 if success, 1 if not found

sub setTemplateV2P {
    my $self = shift;
    my $name = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::setTemplateV2P",1);

    my $templateitem = $self->getTemplate($name);

    if (defined ($templateitem)) {
        $self->{"NEWDB"}->{"configParams"} = $self->{_templates}->getTemplateParameters($templateitem);
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

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.5.0)) {
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

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.5.0)) {
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

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.5.0)) {
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


# Procedure setNoOpenResetLogs
# parameters:
# Set no open database with reset logs after provision

sub setNoOpenResetLogs {
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setNoOpenResetLogs",1);
    $self->{"NEWDB"}->{"openResetlogs"} = JSON::false;
}


# Procedure setNewDBID
# parameters:
# Set new DBID flag to generate a new dbid

sub setNewDBID {
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setNewDBID",1);
    $self->{"NEWDB"}->{"newDBID"} = JSON::true;
}





# Procedure getCustomEnv
# parameters:
# - separator - join using comma or blank
# Return a string with customer env

sub getCustomEnv {
    my $self = shift;
    my $separator = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getCustomEnv",1);

    my $customerenvarray = $self->{"source"}->{"customEnvVars"};
    my $env_nodes;
    my %node_names;


    if ( $self->{_environment}->getType($self->{"environment"}->{"reference"}) eq 'rac' ) {
      $env_nodes = $self->{_environment}->getOracleClusterNode($self->{"environment"}->{"reference"});
      %node_names = map { $_->{reference} => $_->{name} } @{$env_nodes};
    }

    my $ret = '';

    for my $entry (@{$customerenvarray}) {
      if ($entry->{"type"} eq "OracleCustomEnvVarRACFile") {
        my $entry_str = "-customerenvfile \"" . $entry->{"pathParameters"};
        $entry_str = $entry_str . "," . $node_names{$entry->{"clusterNode"}} . " ";
        $ret = join($separator, ($ret, $entry_str));
      }

      if ($entry->{"type"} eq "OracleCustomEnvVarSIFile") {
        my $entry_str = "-customerenvfile \"" . $entry->{"pathParameters"}. "\" ";
        $ret = join($separator, ($ret, $entry_str));
      }

      if ($entry->{"type"} eq "OracleCustomEnvVarRACPair") {
        my $entry_str = "-customerenvpair \"" . $entry->{"varName"} . "\",\"" . $entry->{"varValue"} . "\"";
        $entry_str = $entry_str . "," . $node_names{$entry->{"clusterNode"}} . " ";
        $ret = join($separator, ($ret, $entry_str));
      }

      if ($entry->{"type"} eq "OracleCustomEnvVarSIPair") {
        my $entry_str = "-customerenvpair \"" . $entry->{"varName"} . "\",\"" . $entry->{"varValue"} . "\" ";
        $ret = join($separator, ($ret, $entry_str));
      }

    }


    return $ret;

}



# Procedure setCustomEnv
# parameters:
# - custom_files - array of files
# - custom_pair - array of customer pairs
# Set a customer environment using files

sub setCustomEnv {
    my $self = shift;
    my $custom_files = shift;
    my $custom_pair = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setCustomEnvFiles",1);

    my @customerenvarray;

    my $env_nodes;
    my %node_names;


    if ($self->{'_newenvtype'} eq 'OracleCluster') {
      $env_nodes = $self->{_environment}->getOracleClusterNode($self->{'_newenv'});
      %node_names = map { $_->{name} => $_->{reference} } @{$env_nodes};
    }

    if (defined($custom_files)) {

      for my $file (@{$custom_files}) {
        if ($self->{'_newenvtype'} eq 'OracleCluster') {
          # check if customer file has comma separation with node and if not set it for both nodes
          if ($file =~ /,/) {
            my @t = split(',', $file);
            if (defined($node_names{$t[1]})) {
              # node found by name
              my %entry = (
                "type" => "OracleCustomEnvVarRACFile",
                "clusterNode" => $node_names{$t[1]},
                "pathParameters" => $t[0]
              );
              push(@customerenvarray, \%entry);
            } else {
              # node name not found return error
              print "Node name " . $t[1] . " not found\n";
              return undef;
            }
          } else {
            for my $nodename (keys(%node_names)) {
              my %entry = (
                "type" => "OracleCustomEnvVarRACFile",
                "clusterNode" => $node_names{$nodename},
                "pathParameters" => $file
              );
              push(@customerenvarray, \%entry);
            }
          }
        } else {
          my %entry = (
            "type" => "OracleCustomEnvVarSIFile",
            "pathParameters" => $file
          );
          push(@customerenvarray, \%entry);
        }
      }
    }

    if (defined($custom_pair)) {
      for my $pair (@{$custom_pair}) {


        if ($self->{'_newenvtype'} eq 'OracleCluster') {
          my ($key, $value, $server) = split(',', $pair, 3);
          if (defined($server)) {
            if (defined($node_names{$server})) {
              # node found by name
              my %entry = (
                "type" => "OracleCustomEnvVarRACPair",
                "clusterNode" => $node_names{$server},
                "varName" => $key,
                "varValue" => $value
              );
              push(@customerenvarray, \%entry);
            } else {
              # node name not found return error
              print "Node name " . $server . " not found\n";
              return undef;
            }
          } else {
            for my $nodename (keys(%node_names)) {
              my %entry = (
                "type" => "OracleCustomEnvVarRACPair",
                "clusterNode" => $node_names{$nodename},
                "varName" => $key,
                "varValue" => $value
              );
              push(@customerenvarray, \%entry);
            }
          }
        } else {
          my ($key, $value) = split(',', $pair, 2);
          my %entry = (
            "type" => "OracleCustomEnvVarSIPair",
            "varName" => $key,
            "varValue" => $value
          );
          push(@customerenvarray, \%entry);;
        }

      }


    }

    $self->{"NEWDB"}->{"source"}->{"customEnvVars"} = \@customerenvarray;
    return 1;
}


# Procedure setNoOpen
# parameters:
# Set no open database after v2p

sub setNoOpen {
    my $self = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setNoOpen",1);
    $self->{"NEWDB"}->{"openDatabase"} = JSON::false;
}

# Procedure getLogSync
# parameters: none
# Return status of Log Sync

sub getLogSync
{
    my $self = shift;
    my $ret;
    my $cdbref = $self->getCDBContainerRef();
    my $dbobj;

    if (defined($cdbref)) {
      my $cdbsource = $self->{_source}->getSourceByConfig($cdbref);
      $dbobj = $self->{_databases}->getDB($cdbsource->{container});
    } else {
      $dbobj = $self;
    }

    $ret = $dbobj->{container}->{sourcingPolicy}->{logsyncEnabled} ? 'ACTIVE' : 'INACTIVE';

    logger($self->{_debug}, "Entering VDB_obj::getLogSync",1);
    return $ret;
}

# Procedure getLogSyncMode
# parameters: none
# Return status of Log Sync

sub getLogSyncMode
{
    my $self = shift;
    logger($self->{_debug}, "Entering VDB_obj::getLogSyncMode",1);
    my $ret;
    my $dbobj;
    my $cdbref = $self->getCDBContainerRef();
    if (defined($cdbref)) {
      my $cdbsource = $self->{_source}->getSourceByConfig($cdbref);
      $dbobj = $self->{_databases}->getDB($cdbsource->{container});
    } else {
      $dbobj = $self;
    }

    if (defined($dbobj->{container}->{sourcingPolicy}) ) {
      if ($dbobj->{container}->{sourcingPolicy}->{logsyncMode} eq 'ARCHIVE_ONLY_MODE') {
        $ret = 'arch';
      } elsif ($dbobj->{container}->{sourcingPolicy}->{logsyncMode} eq 'ARCHIVE_REDO_MODE') {
        $ret = 'redo';
      } else {
        $ret = 'unknown';
      }
    } else {
      return 'unknown';
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
    my $logsyncmode = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setLogSync",1);

    my %logsynchash = (
        "type"=> "OracleDatabaseContainer",
        "sourcingPolicy"=> {
            "type"=> "OracleSourcingPolicy"
        }
    );

    if (lc $logsync eq 'yes') {
        $logsynchash{"sourcingPolicy"}{"logsyncEnabled"} = JSON::true;
        if (defined($logsyncmode)) {
          # ARCHIVE_ONLY_MODE  ARCHIVE_REDO_MODE
          if (lc $logsyncmode eq 'arch') {
            $logsynchash{"sourcingPolicy"}{"logsyncMode"} = 'ARCHIVE_ONLY_MODE';
          } elsif (lc $logsyncmode eq 'redo') {
            $logsynchash{"sourcingPolicy"}{"logsyncMode"} = 'ARCHIVE_REDO_MODE';
          } else {
            print "Unknown logsyncmode - exiting\n";
            return undef;
          }
        }

    } else {
        $logsynchash{"sourcingPolicy"}{"logsyncEnabled"} = JSON::false;
    }


    my $ref;

    if (defined($self->{cdb})) {
      $ref = $self->{cdb};
    } else {
      $ref = $self->{container}->{reference};
    }

    my $operation = "resources/json/delphix/database/" . $ref;
    my $payload = to_json(\%logsynchash);

    return $self->runJobOperation($operation,$payload,'ACTION');
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
    my $logsyncmode = shift;

    my $jobno;

    logger($self->{_debug}, "Entering OracleVDB_obj::update_dsource",1);

    if (defined($logsync)) {
      $jobno = $self->setLogSync($logsync, $logsyncmode);
    } else {
      print "Nothing to update\n";
    }

    return $jobno;
}

# Procedure setDSP
# parameters:
#  - numConnections: 1 (*)
#  - compression: false (*)
#  - encryption: false (*)
#  - bandwidthLimit: 0 (*)
# set DSP protocol settings for V2P

sub setDSP
{
    my $self = shift;
    my $numConnections = shift;
    my $compression = shift;
    my $encryption = shift;
    my $bandwidthLimit = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::setDSP",1);

    if (!defined($numConnections)) {
      $numConnections = 1;
    }

    if (!defined($compression)) {
      $compression = JSON::false;
    } else {
      $compression = JSON::true;
    }

    if (!defined($encryption)) {
      $encryption = JSON::false;
    } else {
      $encryption = JSON::true;
    }

    if (!defined($bandwidthLimit)) {
      $bandwidthLimit = 0;
    }

    my %hash_dsp = (
          "type" => "DSPOptions",
          "bandwidthLimit" => $bandwidthLimit,
          "compression" => $compression,
          "encryption" => $encryption,
          "numConnections" => $numConnections
    );


    $self->{"NEWDB"}->{"dspOptions"} = \%hash_dsp;

}



# Procedure setFileParallelism
# parameters:
# - number of concurrent files
# Set no open database after v2p

sub setFileParallelism {
    my $self = shift;
    my $numfiles = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setFileParallelism",1);

    if (!defined($numfiles)) {
      print "Number of concurrent files is not defined";
      return 1;
    } else {
      $self->{"NEWDB"}->{"fileParallelism"} = $numfiles;
      return 0;
    }
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
    my $full = shift;
    my $double = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::snapshot",1);
    my %snapshot_type;

    if  (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.11.0)) {
      %snapshot_type = (
          "type" => "OracleSyncParameters"
      );
    } else {
      # Delphix 6.0
      %snapshot_type = (
          "type" => "OracleSyncFromExternalParameters"
      );
    }

    if (defined($full)) {
      $snapshot_type{"forceFullBackup"} = JSON::true;
    }

    if (defined($double)) {
      $snapshot_type{"doubleSync"} = JSON::true;
    }

    return $self->VDB_obj::snapshot(\%snapshot_type) ;
}


sub setConfig {
    my $self = shift;
    my $name = shift;
    my $source_inst = shift;
    my $source_env = shift;
    my $cdbcont = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::setConfig",1);

    logger($self->{_debug}, "name: " . Dumper $name, 2);
    logger($self->{_debug}, "source_inst: " . Dumper $source_inst, 2);
    logger($self->{_debug}, "source_env: " . Dumper $source_env, 2);
    logger($self->{_debug}, "cdbcont: " . Dumper $cdbcont, 2);
    
    
    my $dlpxObject = $self->{_dlpxObject};
    my $debug = $self->{_debug};

    my $sourceconfig;


    if (!defined($self->{_sourceconfig})) {
        $sourceconfig = new SourceConfig_obj($dlpxObject, $debug);
        $self->{_sourceconfig} = $sourceconfig;
    }

    my $ret;


    if (defined($cdbcont)) {
      my $container_obj = $self->{_sourceconfig}->getSourceConfigByName($cdbcont);
      $ret = $self->{_sourceconfig}->getSourceByCDB($name, $container_obj->{reference});
    } else {
      my $sourceconfig_db = $self->{_sourceconfig}->getSourceConfigByName($name);
      if ($sourceconfig_db->{"type"} ne 'OraclePDBConfig') {
        if (!defined($sourceconfig_db)) {
          print "Source database $name not found\n";
        } else {
          $ret = $sourceconfig_db;
        }
      } else {
        print "Oracle PDB specified without CDB. Please add -cdbcont parameter\n";
      }
    }

    return $ret
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
    my $cdbcont = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::attach_dsource",1);

    my $config = $self->setConfig($source, $source_inst, $source_env, $cdbcont);

    if (! defined($config)) {
        return undef;
    }

    my $source_env_ref = $self->{_repository}->getEnvironment($config->{repository});
    my $source_os_ref = $self->{_environment}->getEnvironmentUserByName($source_env_ref,$source_osuser);

    my $authtype = $self->{_environment}->getEnvironmentUserAuth($source_env_ref, $source_os_ref);

    if ($authtype ne 'kerberos') {
      # assuming we have kerberos and no dbuser is enabled
      if (defined($dbuser)) {
        if ($self->{_sourceconfig}->validateDBCredentials($config->{reference}, $dbuser, $password)) {
            print "Username or password is invalid.\n";
            return undef;
        }
      }
    }




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
    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
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
    } elsif (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.2)) {
      # including to 6.0.2
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
    } elsif (version->parse($self->{_dlpxObject}->getApi()) == version->parse(1.11.3)) {
      # 6.0.3
      %attach_data = (
          "type" => "AttachSourceParameters",
          "attachData" => {
                "type" => "OracleAttachData",
                "config" => $config->{reference},
                "environmentUser" => $source_os_ref
          }
      );

      if (defined($dbuser)) {
        $attach_data{"attachData"}{"oracleFallbackUser"} = $dbuser;
        $attach_data{"attachData"}{"oracleFallbackCredentials"} = $password;
      }

    } else {
      # 6.0.4 and above so far
      %attach_data = (
          "type" => "AttachSourceParameters",
          "attachData" => {
                "type" => "OracleAttachData",
                "config" => $config->{reference},
                "environmentUser" => $source_os_ref
          }
      );

      if (defined($dbuser)) {
        $attach_data{"attachData"}{"oracleFallbackUser"} = $dbuser;
        $attach_data{"attachData"}{"oracleFallbackCredentials"}{"type"} = "PasswordCredential";
        $attach_data{"attachData"}{"oracleFallbackCredentials"}{"password"} = $password;
      }

    }

    if ($config->{type} eq 'OraclePDBConfig') {
      $attach_data{"attachData"}{"type"} = "OraclePDBAttachData";
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
    my $env = shift;
    my @instanceArray;


    my $environments = new Environment_obj($self->{_dlpxObject}, $self->{_debug});
    my $env_nodes = $environments->getOracleClusterNode($env);
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
                    return undef;
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
                return undef;
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


    return \@instanceArray;

    # $self->{"NEWDB"}->{"sourceConfig"}->{"type"} = 'OracleRACConfig';
    # delete $self->{"NEWDB"}->{"sourceConfig"}->{"instance"};
    # $self->{"NEWDB"}->{"sourceConfig"}->{"instances"} = \@instanceArray;
    #
    #
    #
    # return 0;

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
        "type" => $cdb->{"type"},
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


      # sleep to allow change to propagte for next API call ?
      sleep(10);

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
    my $cdbcont = shift;
    my $stagingpush = shift;


    logger($self->{_debug}, "Entering OracleVDB_obj::addSource",1);

    my $config = $self->setConfig($source, $source_inst, $source_env, $cdbcont);

    if (! defined($config)) {
        print "Source database $source not found\n";
        return undef;
    }


    if (defined($dbuser)) {
      if ($self->{_sourceconfig}->validateDBCredentials($config->{reference}, $dbuser, $password)) {
          print "Username or password is invalid or database is down.\n";
          return undef;
      }
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

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
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

    } elsif (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.11.0)) {
        # all above 1.8 below 1.11
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

    } elsif (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.2)) {
      # including Delphix 6.0.2
      %dsource_params = (
        "type" => "LinkParameters",
        "group" => $self->{"NEWDB"}->{"container"}->{"group"},
        "name" => $dsource_name,
        "linkData" => {
            "type" => "OracleLinkFromExternal",
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
        $dsource_params{"linkData"}{"type"} = "OraclePDBLinkFromExternal";
      }

    } elsif (version->parse($self->{_dlpxObject}->getApi()) == version->parse(1.11.3)) {
        # Delphix 6.0.3
        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "OracleLinkFromExternal",
              "config" => $config->{reference},
              "sourcingPolicy" => {
                  "type" => "OracleSourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "oracleFallbackCredentials" => $password,
              "oracleFallbackUser" => $dbuser,
              "environmentUser" => $source_os_ref,
              "linkNow" => JSON::true,
              "compressedLinkingEnabled" => JSON::true
          }
      );

      if ($config->{type} eq 'OraclePDBConfig') {
        $dsource_params{"linkData"}{"type"} = "OraclePDBLinkFromExternal";
      }

    } elsif (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.10)) {
      # Delphix 6.0.4 to 6.0.10

      if (defined($dbuser)) {

        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "OracleLinkFromExternal",
              "config" => $config->{reference},
              "sourcingPolicy" => {
                  "type" => "OracleSourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "oracleFallbackCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
              },
              "oracleFallbackUser" => $dbuser,
              "environmentUser" => $source_os_ref,
              "linkNow" => JSON::true,
              "compressedLinkingEnabled" => JSON::true
          }
        );

      } else {

        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "OracleLinkFromExternal",
              "config" => $config->{reference},
              "sourcingPolicy" => {
                  "type" => "OracleSourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "environmentUser" => $source_os_ref,
              "linkNow" => JSON::true,
              "compressedLinkingEnabled" => JSON::true
          }
        );

      }

      if ($config->{type} eq 'OraclePDBConfig') {
        $dsource_params{"linkData"}{"type"} = "OraclePDBLinkFromExternal";
      }

    } else {
      # Delphix 6.0.11 and higher - so far

      if (defined($dbuser)) {

        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "OracleLinkFromExternal",
              "sourcingPolicy" => {
                  "type" => "OracleSourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "oracleFallbackCredentials" => {
                  "type" => "PasswordCredential",
                  "password" => $password
              },
              "oracleFallbackUser" => $dbuser,
              "environmentUser" => $source_os_ref,
              "linkNow" => JSON::true,
              "compressedLinkingEnabled" => JSON::true,
              "syncStrategy" => {
                  "type" => "OracleSourceBasedSyncStrategy",
                  "config" => $config->{reference}
              }
          }
        );

      } else {

        %dsource_params = (
          "type" => "LinkParameters",
          "group" => $self->{"NEWDB"}->{"container"}->{"group"},
          "name" => $dsource_name,
          "linkData" => {
              "type" => "OracleLinkFromExternal",
              "sourcingPolicy" => {
                  "type" => "OracleSourcingPolicy",
                  "logsyncEnabled" => $logsync_param
              },
              "environmentUser" => $source_os_ref,
              "linkNow" => JSON::true,
              "compressedLinkingEnabled" => JSON::true,
              "syncStrategy" => {
                  "type" => "OracleSourceBasedSyncStrategy",
                  "config" => $config->{reference}
              }
          }
        );

      }

      if ($config->{type} eq 'OraclePDBConfig') {
        $dsource_params{"linkData"}{"type"} = "OraclePDBLinkFromExternal";
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
    my $json_data = to_json(\%dsource_params, {pretty=>1});
    #my $json_data = encode_json(\%dsource_params, pretty=>1);

    logger($self->{_debug}, $json_data, 1);

    return $self->runJobOperation($operation,$json_data, 'ACTION');

}

# Procedure findCDBonEnvironment
# parameters:
# - cdbname
# return sourceconfig ref for CDB name

sub findCDBonEnvironment {
    my $self = shift;
    my $cdbname = shift;

    my $sourceconfig = new SourceConfig_obj($self->{_dlpxObject}, $self->{_debug});

    my $cdbconf;

    if (defined($cdbname)) {
      my $cdbobj = $sourceconfig->getSourceConfigByName($cdbname);
      if (defined($cdbobj)) {
        if ($cdbobj->{cdbType} ne 'ROOT_CDB') {
          print("Database is found but this is not discovered as CDB container. Please add cdbuser/cdbpass to run discovery during provision\n");
          return undef;
        }
        $cdbconf = $cdbobj->{reference};
      } else {
        print "CDB named $cdbname not found in Oracle Home and environment\n";
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
# - vcdbname
# - vcdbgroup
# - vcdbdbname
# - vcdbinstname
# - vcdbuniqname
# - vcdbtemplate
# Setup a virtual CDB


sub setupVCDB {

    my $self = shift;
    my $vcdbname = shift;
    my $vcdbgroup = shift;
    my $vcdbdbname = shift;
    my $vcdbinstname = shift;
    my $vcdbuniqname = shift;
    my $vcdbtemplate = shift;
    my $vcdbrac_instance = shift;
    my $vcdbtdepassword = shift;
    my $vcdbtdekeystore = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::setupVCDB",1);

    $self->{_vcdbname} = $vcdbname;
    $self->{_vcdbgroup} = $vcdbgroup;
    $self->{_vcdbdbname} = $vcdbdbname;
    $self->{_vcdbinstname} = $vcdbinstname;
    $self->{_vcdbuniqname} = $vcdbuniqname;
    $self->{_vcdbtemplate} = $vcdbtemplate;
    $self->{_vcdbtemplate} = $vcdbtemplate;
    $self->{_vcdbrac_instance} = $vcdbrac_instance;

    if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.11.18)) {
      $self->{_vcdbtdepassword} = $vcdbtdepassword;
      $self->{_vcdbtdekeystore} = $vcdbtdekeystore;
    }

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
      # target environment is RAC

      if ($self->{_sourcedb}->{container}->{contentType} eq "NON_CDB") {
        # non PDB RAC
        my $instance_array = $self->setRacProvisioning($instances, $self->{'_newenv'} );
        if ( ! defined($instance_array) ) {
            print "Problem with node names or instance numbers. Please double check.";
            return undef;
        }

        $self->{"NEWDB"}->{"sourceConfig"}->{"type"} = 'OracleRACConfig';
        delete $self->{"NEWDB"}->{"sourceConfig"}->{"instance"};
        $self->{"NEWDB"}->{"sourceConfig"}->{"instances"} = $instance_array;

      } else {
        $self->{"NEWDB"}->{"sourceConfig"}->{"type"} = "OraclePDBConfig";
      }


    } else {
      # target host is not RAC
      # check config type of Parent
      my $configtype = $self->{_sourcedb}->getSourceConfigType();


      if ($configtype eq 'OracleRACConfig') {
        # source was RAC but target enviroment is not RAC
        $configtype = "OracleSIConfig";
      } elsif ($configtype eq 'N/A') {
        # detached - check source DB container type
        logger($self->{_debug}, "SourceDB container type " . Dumper $self->{_sourcedb}->{container}->{contentType});
        if ($self->{_sourcedb}->{container}->{contentType} eq "NON_CDB") {
          $configtype = "OracleSIConfig";
        } else {
          # set to PDB
          $configtype = "OraclePDBConfig";
        }

      }
      $self->{"NEWDB"}->{"sourceConfig"}->{"type"} = $configtype;


    }

    #print Dumper $self->{"NEWDB"}->{"sourceConfig"};
    #exit;

    logger($self->{_debug}, "Target sourceConfig type " . Dumper $self->{"NEWDB"}->{"sourceConfig"}->{"type"}, 2 );

    if ( $self->{"NEWDB"}->{"sourceConfig"}->{"type"} eq 'OraclePDBConfig') {
      if (!(defined($cdbname) || defined($self->{_vcdbname}) ) ) {
        print "Container name (-cdb) or virtual CDB settings has to be set for vPDB provisioning. VDB won't be created\n";
        return undef;
      }

      if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.9.0)) {
        $self->{"NEWDB"}->{"type"} = "OracleMultitenantProvisionParameters";
        $self->{"NEWDB"}->{"source"}->{"type"} = "OracleVirtualPdbSource";
      }

      # clean up a instance parameter for vPDB
      delete $self->{"NEWDB"}->{"sourceConfig"}->{"instance"};
      delete $self->{"NEWDB"}->{"sourceConfig"}->{"uniqueName"};
      delete $self->{"NEWDB"}->{"sourceConfig"}->{"services"};

      if (defined($cdbname)) {
        # provision to existing CDB
        my $cdbconf = $self->findCDBonEnvironment($cdbname);
        if (!(defined($cdbconf))) {
          return undef;
        }

        $self->{"NEWDB"}->{"sourceConfig"}->{"cdbConfig"} = $cdbconf;
      } else {
        # creating a vCDB

        if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
          print "Virtual CDB is supported in Delphix Engine 5.2 or higher\n";
          return undef;
        }

        my $vcdbgroupref;
        if (defined($self->{_vcdbgroup})) {
          if (defined($self->{_groups}->getGroupByName($self->{_vcdbgroup}))) {
            $vcdbgroupref = $self->{_groups}->getGroupByName($self->{_vcdbgroup})->{reference};
          } else {
            print "Group for vcdb - " . $self->{_vcdbgroup} . " not found. VDB won't be created\n";
            return undef;
          }
        } else {
          $vcdbgroupref = $self->{"NEWDB"}->{"container"}->{"group"};
        }

        my $vcdbinstname;
        my $vcdbuniqname;

        if (defined($self->{_vcdbuniqname})) {
          $vcdbuniqname = $self->{_vcdbuniqname};
        } else {
          $vcdbuniqname = $self->{_vcdbdbname};
        }

        if (defined($self->{_vcdbinstname})) {
          $vcdbinstname = $self->{_vcdbinstname};
        } else {
          $vcdbinstname = $self->{_vcdbdbname};
        }


        my %virtcdbhash = (
          "type" => "OracleVirtualCdbProvisionParameters",
          "container" => {
              "type" => "OracleDatabaseContainer",
              "name" => $self->{_vcdbname},
              "group" => $vcdbgroupref
          },
          "source" => {
              "type" => "OracleVirtualCdbSource",
              "mountBase" => $self->{"NEWDB"}->{"source"}->{"mountBase"},
              "allowAutoVDBRestartOnHostReboot" => $self->{"NEWDB"}->{"source"}->{"allowAutoVDBRestartOnHostReboot"}
          },
          "sourceConfig" => {
              "type" => "OracleSIConfig",
              "repository" => $self->{"NEWDB"}->{"sourceConfig"}->{"repository"},
              "databaseName" => $self->{_vcdbdbname},
              "uniqueName" => $vcdbuniqname
          }
        );


        if (defined($self->{_vcdbrac_instance})) {
          # vcdb is RAC
          my $instance_array = $self->setRacProvisioning($self->{_vcdbrac_instance}, $self->{'_newenv'} );
          if ( ! defined($instance_array) ) {
              print "Problem with node names or instance numbers. Please double check.";
              return undef;
          }
          $virtcdbhash{"sourceConfig"}{"type"} = "OracleRACConfig";
          $virtcdbhash{"sourceConfig"}{"instances"} = $instance_array;
        } else {
          # vcdb is  not RAC
          $virtcdbhash{"sourceConfig"}{"instance"} = {
              "type" => "OracleInstance",
              "instanceNumber" => 1,
              "instanceName" => $vcdbinstname
          };

        }


        if (defined($self->{_vcdbtemplate})) {
          my $vcdbtemplateref = $self->getTemplate($self->{_vcdbtemplate});
          if (!defined($vcdbtemplateref)) {
            print "Template for vCDB template name " . $self->{_vcdbtemplate} . " not found. VDB won't be created\n";
            return undef;
          }
          $virtcdbhash{"source"}{"configTemplate"} = $vcdbtemplateref;
        }


        if (defined($self->{_vcdbtdepassword})) {
          $self->{"NEWDB"}{"source"}{"targetVcdbTdeKeystorePath"} = $self->{_vcdbtdekeystore};
          $virtcdbhash{"sourceConfig"}{"tdeKeystorePassword"} = $self->{_vcdbtdepassword};
        }
        

        $self->{"NEWDB"}->{"virtualCdb"} = \%virtcdbhash;

      }
    }


    my $operation = 'resources/json/delphix/database/provision';
    my $json_data = $self->getJSON();
    return $self->runJobOperation($operation,$json_data);

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
    my $useabsolute = shift;

    logger($self->{_debug}, "Entering VDB_obj::setFileSystemLayout",1);



    if (version->parse($self->{_dlpxObject}->getApi()) <= version->parse(1.11.9)) {
      $self->{"NEWDB"}->{"filesystemLayout"}->{"type"} = "TimeflowFilesystemLayout";
    } else {
      # from Delphix 6.0.10 and above
      $self->{"NEWDB"}->{"filesystemLayout"}->{"type"} = "OracleExportTimeflowFilesystemLayout";
    }

    if (defined($targetDirectory)) {
        $self->{"NEWDB"}->{"filesystemLayout"}->{"targetDirectory"} = $targetDirectory;
    }

    

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

    if ( defined($useabsolute)) {
      $self->{"NEWDB"}->{"filesystemLayout"}->{"useAbsolutePathForDataFiles"} = JSON::true;
    }

    return 0;

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





# Procedure setupTDE
# parameters:
# - tdeparentpassword - parent TDE keystore password
# - tdeparentpath - parent TDE keystore path
# - tdeexportsecret - TDE export
# - tdekeyid - TDE keyid
# Setup TDE for Oracle MT

sub setupTDE {
    my $self = shift;
    my $tdeparentpassword = shift;
    my $tdeparentpath = shift;
    my $tdeexportsecret = shift;
    my $tdekeyid = shift;

    logger($self->{_debug}, "Entering OracleVDB_obj::setupTDE",1);

    if (version->parse($self->{_dlpxObject}->getApi()) > version->parse(1.11.10)) {

      if (defined($tdeparentpassword)) {
        $self->{"NEWDB"}->{"source"}->{"parentTdeKeystorePassword"} = $tdeparentpassword;
      }

      if (defined($tdeparentpath)) {
        $self->{"NEWDB"}->{"source"}->{"parentTdeKeystorePath"} = $tdeparentpath;
      }

      if (defined($tdeexportsecret)) {
        $self->{"NEWDB"}->{"source"}->{"tdeExportedKeyFileSecret"} = $tdeexportsecret;
      }

      if (defined($tdekeyid)) {
        $self->{"NEWDB"}->{"source"}->{"tdeKeyIdentifier"} = $tdekeyid;
      }

      return 0;

    } else {
      print "Error - native support for Oracle MT TDE requires engine version 6.0.8 or higher";
      return 1;
    }



}

# Procedure getTDE
# parameters:
# - separator - join using comma or blank
# Return a string with TDE parameters

sub getTDE {
    my $self = shift;
    my $separator = shift;
    my $vcdb = shift;
    logger($self->{_debug}, "Entering OracleVDB_obj::getTDE",1);
    my $ret = '';
    if (defined($self->{"source"}->{"parentTdeKeystorePath"})) {
       $ret = " -tdeparentpath " . $self->{"source"}->{"parentTdeKeystorePath"};
       $ret = $ret . " -tdeparentpassword xxxxxx -tdeexportsecret xxxxxxx ";
       if (defined($vcdb)) {
       } else {
        $ret = $ret . "-tdecdbpassword xxxxxxx";
       }
       if (defined($self->{"source"}->{"tdeKeyIdentifier"})) {
         $ret = $ret . " -tdekeyid " . $self->{"source"}->{"tdeKeyIdentifier"};
       }
    }

    return $ret;
}


# Procedure getStagingPush
# parameters:
# Return is staging push is configured

sub getStagingPush {
  my $self = shift;
  logger($self->{_debug}, "Entering OracleVDB_obj::getStagingPush",1);
  my $ret = 'N/A';

  # if (version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.11.10)) {
  #     # 6.0.11 and above
  #     if (defined($self->{source}->{syncStrategy})) {
  #       if ($self->{source}->{syncStrategy}->{type} eq 'MSSqlStagingPushSyncStrategy') {
  #         $ret = 'yes';
  #       } else {
  #         $ret = 'no';
  #       }
  #     }

  # } else {
  #   $ret = 'N/A';
  # }

  return $ret;

}

#######################
# end of OracleVDB_obj class
#######################

1;
