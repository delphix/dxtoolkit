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
# Program Name : SourceConfig_obj.pm
# Description  : Delphix Engine Source object
# It's include the following classes:
# - SourceConfig_obj - class which map a Delphix Engine source config API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#


package SourceConfig_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering SourceConfig_obj::constructor",1);

    my %sourceconfigs;
    my $self = {
        _sourceconfigs => \%sourceconfigs,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->getSourceConfigList($debug);
    return $self;
}

# Procedure refresh
# Refresh source config

sub refresh {
  my $self = shift;
  logger($self->{_debug}, "Entering SourceConfig_obj::refresh",1);
  $self->getSourceConfigList();
}



# Procedure getSourceConfig
# parameters:
# - reference - reference of source config
# Return source config hash for specific source config reference

sub getSourceConfig {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::getSourceConfig",1);

    my $sourceconfigs = $self->{_sourceconfigs};

    my $ret;

    if (defined($reference)) {
        if ($reference eq 'NA') {
          $ret = 'NA';
        } elsif (defined($sourceconfigs->{$reference})) {
            $ret = $sourceconfigs->{$reference};
        } else {
            $ret = 'NA';
        }
    } else {
        $ret = 'NA';
    }
    return $ret;
}

# Procedure getType
# parameters:
# - reference
# Return source type for specific source reference

sub getType {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::getType",1);

    my $sourceconfigs = $self->{_sourceconfigs};
    return $sourceconfigs->{$reference}->{type};
}

# Procedure getDBUser
# parameters:
# - reference
# Return username for specific reference

sub getDBUser {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::getDBUser",1);

    my $sourceconfigs = $self->{_sourceconfigs};
    return $sourceconfigs->{$reference}->{user};
}

# Procedure getName
# parameters:
# - reference
# Return source name for specific source reference

sub getName {
    my $self = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::getName",1);

    my $sourceconfigs = $self->{_sourceconfigs};
    return $sourceconfigs->{$container}->{name};
}

# Procedure getSourceConfigByNameForRepo
# parameters:
# - name
# - repo reference
# Return source hash for specific source name

sub getSourceConfigByNameForRepo {
    my $self = shift;
    my $name = shift;
    my $repo = shift;
    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::getSourceConfigByNameForRepo",1);

    for my $sourceitem ( sort ( keys %{$self->{_sourceconfigs}} ) ) {

        if ($sourceitem ne 'NA') {
          if ( ( $self->getName($sourceitem) eq $name ) && ( $self->getRepository($sourceitem) eq $repo ) ) {
              $ret = $self->getSourceConfig($sourceitem);
          }
        }
    }

    return $ret;
}

# Procedure getSourceConfigsListForRepo
# parameters:
# - repo reference
# Return source config list for repository

sub getSourceConfigsListForRepo {
    my $self = shift;
    my $repo = shift;
    my $ret;
    my @retarray;

    logger($self->{_debug}, "Entering SourceConfig_obj::getSourceConfigsListForRepo",1);

    for my $sourceitem ( sort ( keys %{$self->{_sourceconfigs}} ) ) {

        if ($sourceitem ne 'NA') {
          if ( $self->getRepository($sourceitem) eq $repo ) {
              push(@retarray, $sourceitem);
          }
        }
    }

    return \@retarray;
}


# Procedure getSourceByName
# parameters:
# - name
# Return source hash for specific source name

sub getSourceConfigByName {
    my $self = shift;
    my $name = shift;
    my $repo = shift;
    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::getSourceConfigByName",1);

    for my $sourceitem ( sort ( keys %{$self->{_sourceconfigs}} ) ) {

        if ( defined($self->getName($sourceitem))  && ( $self->getName($sourceitem) eq $name  )) {
            $ret = $self->getSourceConfig($sourceitem);
        }

    }

    return $ret;
}

# Procedure getSourceByCDB
# parameters:
# - name
# - cdb ref
# Return source hash for specific source name

sub getSourceByCDB {
    my $self = shift;
    my $name = shift;
    my $cdb = shift;
    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::getSourceByCDB",1);

    for my $sourceitem ( sort ( keys %{$self->{_sourceconfigs}} ) ) {

        if ( defined($self->getName($sourceitem))  && ( $self->getName($sourceitem) eq $name  )
             && defined($self->{_sourceconfigs}->{$sourceitem}->{cdbConfig}) 
             && ($self->{_sourceconfigs}->{$sourceitem}->{cdbConfig} eq $cdb)  ) {
            $ret = $self->getSourceConfig($sourceitem);
        }

    }

    return $ret;
}


# Procedure validateDBCredentials
# parameters:
# - username
# - password
# Return status of credential check ( 0 - OK )

sub validateDBCredentials {
    my $self = shift;
    my $reference = shift;
    my $username = shift;
    my $password = shift;
    my $dbusertype = shift;
    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::validateDBCredentials",1);

    my %sourceconfig_hash;

    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.11.2)) {
      %sourceconfig_hash = (
          "type" => "SourceConfigConnectivity",
          "password" => $password,
          "username" => $username
      );
    } else {
      # 6.0.2 onwards, if we are on MS SQL do a job

      if ($self->{_sourceconfigs}->{$reference}->{"type"} =~ /MSSql.*/) {
        if (!defined($dbusertype)) {
          print "MS SQL database user type is now required\n";
          return 1;
        }
        %sourceconfig_hash = (
            "type" => "MSSqlSourceConfigConnectivity",
            "mssqlUser" => {}
        );

        $sourceconfig_hash{"mssqlUser"}{"user"} = $username;
        if (lc $dbusertype eq 'database') {
          $sourceconfig_hash{"mssqlUser"}{"type"} = "MSSqlDatabaseUser";
          $sourceconfig_hash{"mssqlUser"}{"password"} = {
                            "type" => "PasswordCredential",
                            "password" => $password
                          };
        } elsif (lc $dbusertype eq "environment") {
          $sourceconfig_hash{"mssqlUser"}{"type"} = "MSSqlEnvironmentUser";
        } elsif (lc $dbusertype eq "domain") {
          $sourceconfig_hash{"mssqlUser"}{"type"} = "MSSqlDomainUser";
          $sourceconfig_hash{"mssqlUser"}{"password"} = {
                            "type" => "PasswordCredential",
                            "password" => $password
                          };

        }
      } else {
        %sourceconfig_hash = (
            "type" => "SourceConfigConnectivity",
            "password" => $password,
            "username" => $username
        );
      }

    }

    my $json_data = encode_json(\%sourceconfig_hash);

    my $operation = 'resources/json/delphix/sourceconfig/' . $reference . '/validateCredentials';

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        $ret = 1;
    }

    return $ret;
}

# Procedure setCredentials
# parameters:
# - username
# - password
# - force - if defined skip check
# Return status of credential check ( 0 - OK )

sub setCredentials {
    my $self = shift;
    my $reference = shift;
    my $username = shift;
    my $password = shift;
    my $force = shift;
    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::setCredentials",1);

    if (!defined($force)) {
        if (!defined($username)) {
            $username = $self->getDBUser($reference);
        }

        if ($self->{_sourceconfigs}->{$reference}->{"type"} =~ /MSSql.*/) {
          # database type is hardcoded - there is no password for other types
          if ($self->validateDBCredentials($reference, $username, $password, 'database')) {
              print "Password check failed.\n";
              return 1;
          }
        } else {
          if ($self->validateDBCredentials($reference, $username, $password)) {
              print "Password check failed.\n";
              return 1;
          }
        }
    }

    my %sourceconfig_hash;


    if (defined($username)) {
        %sourceconfig_hash = (
            "type" => $self->getType($reference),
            "user" => $username,
             "credentials" => {
                "type" => "PasswordCredential",
                "password" => $password
            }
        );
    } else {
        %sourceconfig_hash = (
            "type" => $self->getType($reference),
             "credentials" => {
                "type" => "PasswordCredential",
                "password" => $password
            }
        );
    }



    my $json_data = encode_json(\%sourceconfig_hash);

    my $operation = 'resources/json/delphix/sourceconfig/' . $reference ;

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        $ret = 1;
    }

    return $ret;
}


# Procedure getRepository
# parameters:
# - reference - reference of source config
# Return repository reference for specific source config reference

sub getRepository {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::getRepository",1);

    my $sourceconfigs = $self->{_sourceconfigs};
    my $ret;

    if (defined($reference)) {
        $ret = $sourceconfigs->{$reference}->{'repository'};
    } else {
        $ret = 'NA';
    }
    return $ret;
}


# Procedure getSourceConfigList
# parameters: - none
# Load list of sources config objects from Delphix Engine

sub getSourceConfigList
{
    my $self = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::getSourceConfigList",1);
    my $operation = "resources/json/delphix/sourceconfig";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    #print Dumper $result_fmt;
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {

        my @res = @{$result->{result}};

        my $sourceconfigs = $self->{_sourceconfigs};


        for my $scitem (@res) {
            $sourceconfigs->{$scitem->{reference}} = $scitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }


}


sub buildOracleSourceConfig {
    my $self = shift;
    my $type = shift;
    my $repository = shift;
    my $dbname = shift;
    my $native_params = shift;

    my ($jdbc, $uniquename, $instancename);

    if (defined($native_params->{"jdbc"})) {
      $jdbc = $native_params->{"jdbc"};
    } else {
      print "JDBC parameter is missing\n";
      return 1;
    }

    if (defined($native_params->{"uniquename"})) {
      $uniquename = $native_params->{"uniquename"};
    } else {
      print "uniquename parameter is missing\n";
      return 1;
    }

    if (defined($native_params->{"instancename"})) {
      $instancename = $native_params->{"instancename"};
    } else {
      print "instancename parameter is missing\n";
      return 1;
    }

    my @services;
    my %service = (
      "type" => "OracleService",
      "jdbcConnectionString" => "jdbc:oracle:thin:@" . $jdbc
    );

    push(@services, \%service);

    my %sourceconfig_hash = (
        "type" => "OracleSIConfig",
        "repository" => $repository,
        "services" => \@services,
        "databaseName" => $dbname,
        "uniqueName" => $uniquename,
        "instance" => {
            "type" => "OracleInstance",
            "instanceName" => $instancename,
            "instanceNumber" => 1
        }
      );

      return \%sourceconfig_hash;
}


sub buildVfilesSourceConfig {
    my $self = shift;
    my $type = shift;
    my $repository = shift;
    my $dbname = shift;
    my $native_params = shift;

    my $path;

    if (defined($native_params->{"path"})) {
      $path = $native_params->{"path"};
    } else {
      print "Path parameter is missing\n";
      return 1;
    }

    my %sourceconfig_hash = (
      "type" => "AppDataDirectSourceConfig",
      "repository" => $repository,
      "name" => $dbname,
      "path" => $path
    );

    return \%sourceconfig_hash;
}



sub buildpluginSourceConfig {
    my $self = shift;
    my $type = shift;
    my $repository = shift;
    my $dbname = shift;
    my $native_params = shift;
    my $plugin_params = shift;

    my %sourceconfig_hash = (
      "type" => "AppDataStagedSourceConfig",
      "repository" => $repository,
      "name" => $dbname,
      "linkingEnabled" => JSON::true,
      "parameters" => $plugin_params
    );

    return \%sourceconfig_hash;
}


# Procedure createSourceConfig
# parameters:
# - type
# - repository
# - dbname
# - native_params
# - plugin_params
# Create a SourceConfig ( database to be added as dSource )
# Return 0 if OK

sub createSourceConfig {
    my $self = shift;
    my $type = shift;
    my $repository = shift;
    my $dbname = shift;
    my $native_params = shift;
    my $plugin_params = shift;

    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::createSourceConfig",1);


    my $sourceconfig_hash;

    if ($type eq 'oracleSI') {
      $sourceconfig_hash = $self->buildOracleSourceConfig($type, $repository, $dbname, $native_params);
    } elsif ($type eq 'vfiles') {
      $sourceconfig_hash = $self->buildVfilesSourceConfig($type, $repository, $dbname, $native_params);
    } elsif ($type eq 'plugin') {
      $sourceconfig_hash = $self->buildpluginSourceConfig($type, $repository, $dbname, $native_params, $plugin_params);
    } else {
      return 1;
    }


    my $json_data = encode_json($sourceconfig_hash);

    my $operation = 'resources/json/delphix/sourceconfig';

    logger($self->{_debug}, $json_data ,2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with adding database " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
        $ret = 1;
    }

    return $ret;
}

# Procedure deleteSourceConfig
# parameters:
# - name
# - repository
# Drop an SourceConfig with specific name ( database to be added as dSource )
# Return 0 if OK

sub deleteSourceConfig {
    my $self = shift;
    my $name = shift;
    my $repository = shift;
    my $ret;

    logger($self->{_debug}, "Entering SourceConfig_obj::createSourceConfig",1);

    my $obj = $self->getSourceConfigByNameForRepo($name, $repository);

    my $ref = $obj->{reference};

    if (!defined($ref)) {
      print "Database $name not found\n";
      return 1;
    }

    my $operation = 'resources/json/delphix/sourceconfig/' . $ref . '/delete';


    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, '{}');

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with deleting database " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
        $ret = 1;
    }

    return $ret;

}

# Procedure update_cdb_tde_password
# parameters:
# - tde_password - TDE Keystore password
# Set a password for keystore
# Return jobno

sub update_cdb_tde_password {
    my $self = shift;
    my $reference = shift;
    my $tde_password = shift;

    logger($self->{_debug}, "Entering SourceConfig_obj::update_cdb_tde_password",1);

    my %sourceconfig_hash;
    my $jobno;


    %sourceconfig_hash = (
        "type" => $self->getType($reference)
    );


    if (defined($tde_password)) {
      $sourceconfig_hash{"tdeKeystorePassword"} = $tde_password;
      my $json_data = to_json(\%sourceconfig_hash);

      logger($self->{_debug}, $json_data ,2);

      my $operation = 'resources/json/delphix/sourceconfig/' . $reference;
      $jobno = $self->runJobOperation($operation, $json_data, 'ACTION');
    } else {
      print "No TDE password to set\n";
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


1;
