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
# Program Name : Environment_obj.pm
# Description  : Delphix Engine environment object
# It's include the following classes:
# - Environment_obj - class which map a Delphix Engine environment API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
#


package Environment_obj;

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
    logger($debug, "Entering Environment_obj::constructor",1);

    my %environments;
    my %envusers;
    my %envlisteners;
    my $self = {
        _environments => \%environments,
        _envusers => \%envusers,
        _envlisteners => \%envlisteners,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->getEnvironmentList(1);
    $self->getEnvironmentUsers();
    $self->getEnvironmentListeners();
    return $self;
}


# Procedure getAllEnvironments
# parameters: none
# Return list of environments (references)

sub getAllEnvironments {
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::getAllEnvironments",1);


    my @mainenv = grep { $self->{_environments}->{$_}->{type} ne 'OracleClusterNode' } keys %{$self->{_environments}};

    return sort ( @mainenv );
}


# Procedure getEnvironment
# parameters:
# - reference
# Return environment hash for specific environment reference


sub getEnvironment {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironment",1);

    my $environments = $self->{_environments};
    return $environments->{$reference};
}


# Procedure getEnvironmentUser
# parameters:
# - reference
# Return environment hash for specific environment reference


sub getEnvironmentUser {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUser",1);

    my $envusers = $self->{_envusers};
    return $envusers->{$reference};
}

# Procedure getProxy
# parameters:
# - reference
# Return environment primary user for specific environment reference

sub getProxy {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getProxy",1);

    my $environments = $self->{_environments};
    return $environments->{$reference}->{proxy} ? $environments->{$reference}->{proxy} : 'N/A';
}

# Procedure getPrimaryUser
# parameters:
# - reference
# Return environment primary user for specific environment reference

sub getPrimaryUser {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getPrimaryUser",1);

    my $environments = $self->{_environments};
    my $ret;
    
    if (defined($environments->{$reference})) {
      $ret = $environments->{$reference}->{primaryUser};
    } else {
      $ret = 'N/A';
    }
    
    return $ret;
}

# Procedure getPrimaryUserName
# parameters:
# - reference
# Return environment primary user for specific environment reference

sub getPrimaryUserName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getPrimaryUser",1);

    my $environments = $self->{_environments};


    #my $username = $self->{_envusers}->{name};
    my $ret;
    
    if (defined($environments->{$reference}->{_primaryUserName})) {
      $ret = $environments->{$reference}->{_primaryUserName};
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure getPrimaryUserAuth
# parameters:
# - reference
# Return environment primary user authtype for specific environment reference

sub getPrimaryUserAuth {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getPrimaryUserAuth",1);

    my $environments = $self->{_environments};


    #my $username = $self->{_envusers}->{name};



    my $ret = $environments->{$reference}->{_primaryUserAuth};
    if ($ret eq 'PasswordCredential') {
      $ret = 'password';
    } elsif ($ret eq 'KeyPairCredential') {
      $ret = 'systemkey';
    }

    return $ret;
}

# Procedure getName
# parameters:
# - reference
# Return environment name for specific environment reference

sub getName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getName",1);

    my $environments = $self->{_environments};
    return $environments->{$reference}->{'name'};
}

# Procedure getStatus
# parameters:
# - reference
# Return environment status for specific environment reference

sub getStatus {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getStatus",1);

    my $environments = $self->{_environments};
    return $environments->{$reference}->{'enabled'} ? 'enabled' : 'disabled';
}

# Procedure getType
# parameters:
# - reference
# Return environment status for specific environment reference

sub getType {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getType",1);

    my $environments = $self->{_environments};
    my $ret = $environments->{$reference}->{'type'};
    if ($ret eq 'UnixHostEnvironment') {
      $ret = 'unix';
    } elsif ($ret eq 'WindowsHostEnvironment') {
      $ret = 'windows';
    }

    return $ret;
}


# Procedure getHost
# parameters:
# - reference
# Return host reference for specific environment reference

sub getHost {
    my $self = shift;
    my $reference = shift;
    my $ret;

    logger($self->{_debug}, "Entering Environment_obj::getHost",1);

    my $environments = $self->{_environments};
    if (defined($reference) && defined($environments->{$reference}) ) {
        if ( $environments->{$reference}->{'type'} eq 'OracleCluster') {
            $ret = 'CLUSTER';
        } elsif ( $environments->{$reference}->{'type'} eq 'WindowsCluster' )   {
            $ret = 'CLUSTER';
        } else {
            $ret = $environments->{$reference}->{'host'};
        }
    } else {
        $ret = 'NA';
    }

    return $ret;
}


# Procedure getEnvironmentByName
# parameters:
# - name - repository name
# Return environment reference for environment name

sub getEnvironmentByName {
    my $self = shift;
    my $name = shift;
    my $ret;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentByName",1);

    for my $envitem ( sort ( keys %{$self->{_environments}} ) ) {

        if ( $self->getName($envitem) eq $name) {
            $ret = $self->getEnvironment($envitem);
        }
    }

    return $ret;
}

# Procedure getEnvironmentUserByRef
# parameters:
# - name - repository refrence
# - user - user ref
# Return environment user reference for environment name and user name

sub getEnvironmentUserByRef {
    my $self = shift;
    my $ref = shift;
    my $user = shift;
    my $ret;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUserByRef",1);

    if (defined($self->{_environments}->{$ref})) {
        # is this a environment refrerence
        my $users = $self->getEnvironmentUser($ref);
        if (defined($users->{$user})) {
          $ret = $users->{$user}->{name};
        } else {
          $ret = 'N/A';
        }
    }
    
    return $ret;
}


# Procedure getEnvironmentUserByName
# parameters:
# - name - repository name or refrence
# - username - user name
# Return environment user reference for environment name and user name

sub getEnvironmentUserByName {
    my $self = shift;
    my $name = shift;
    my $username = shift;
    my $ret;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUserByName",1);

    if (defined($self->{_environments}->{$name})) {
        # is this a environment refrerence
        my $users = $self->getEnvironmentUser($name);
        
        my @t = grep { $users->{$_}->{name} eq $username } keys %{$users};
        # if (defined($users->{$username})) {
        #     $ret = $users->{$username}->{reference};
        # }
        
        if (scalar(@t) > 1) {
          print "Too many users found\n";
        } else {
          $ret = $t[-1];
        }
        
    } else {

        for my $envitem ( sort ( keys %{$self->{_environments}} ) ) {

            if ( $self->getName($envitem) eq $name) {
                my $users = $self->getEnvironmentUser($envitem);
                # if (defined($users->{$username})) {
                #     $ret = $users->{$username}->{reference};
                # }
                my @t = grep { $users->{$_}->{name} eq $username } keys %{$users};
                if (scalar(@t) > 1) {
                  print "Too many users found\n";
                } else {
                  $ret = $t[-1];
                }
            }
        }
    }

    return $ret;
}


# Procedure getOracleClusterNode
# parameters:
# - ref - environment reference
# Return environment reference for environment name

sub getOracleClusterNode {
    my $self = shift;
    my $ref = shift;
    my $ret;

    logger($self->{_debug}, "Entering Environment_obj::getOracleClusterNode",1);

    my $operation = "resources/json/delphix/environment/oracle/clusternode?cluster=" . $ref;
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);


    if (defined $result->{result}) {
        $ret = $result->{result}
    }

    return $ret;
}

# Procedure getEnvironmentList
# parameters: none
# -primary - get primary only
# Load a list of environment objects from Delphix Engine

sub getEnvironmentList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentList",1);

    my $operation = "resources/json/delphix/environment";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $environments = $self->{_environments};
        for my $envitem (@res) {
            if (defined($envitem->{namespace})) {
              #skip replicated env
              next;
            }
            $environments->{$envitem->{reference}} = $envitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

    $operation = "resources/json/delphix/environment/oracle/clusternode";
    ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $environments = $self->{_environments};
        for my $envitem (@res) {
            $environments->{$envitem->{reference}} = $envitem;
        }
    }


}

# Procedure getEnvironmentUsers
# parameters: none
# Load a list of users for all environment objects from Delphix Engine

sub getEnvironmentUsers
{
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUsers",1);

    my $operation = "resources/json/delphix/environment/user";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $envusers = $self->{_envusers};
        for my $envuser (@res) {
            if (defined($envuser->{namespace})) {
              next;
            }
            $envusers->{$envuser->{environment}}->{$envuser->{reference}} = $envuser;
                        
            if ($self->getPrimaryUser($envuser->{environment}) eq $envuser->{reference} ) {
              $self->{_environments}->{$envuser->{environment}}->{_primaryUserName} = $envuser->{name};
              $self->{_environments}->{$envuser->{environment}}->{_primaryUserAuth} = $envuser->{credential}->{type};
            }
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}


#procedure getEnvironmentListenerPorts
#parameters:
# - ref - environment
#return array of listener ports

sub getEnvironmentListenerPorts {
    my $self = shift;
    my $ref = shift;
    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentListenerPorts",1);

    my $envlisteners = $self->{_envlisteners};

    my %ports;

    if (defined($envlisteners->{$ref})) {

        for my $listenv (sort keys %{$envlisteners->{$ref}} ) {
            my $listarray = $envlisteners->{$ref}->{$listenv}->{endPoints};

            if (defined($listarray)) {
                for my $list (@{$listarray}) {
                    my $port = (split(':',$list))[1];
                    $ports{$port}=1;
                }
            }
        }


    }

    return sort keys %ports;

}


#procedure getListenerByName
#parameters: 
# - env refrence
# - list name
#return listener refrence for name

sub getListenerByName {
    my $self = shift;
    my $envref = shift;
    my $listname = shift;
    logger($self->{_debug}, "Entering Environment_obj::getListenerByName",1);
    my $envlisteners = $self->{_envlisteners};
    
    my $ret;
    
    my @listref = grep { lc $envlisteners->{$envref}->{$_}->{name} eq lc $listname } keys %{$envlisteners->{$envref}};
    
    if (scalar(@listref) eq 1) {
      $ret = $listref[-1];
    };
    
    return $ret;
  
}



#procedure getListenerName
#parameters: 
# - env refrence
# - list reference
#return listener name for refrence

sub getListenerName {
    my $self = shift;
    my $envref = shift;
    my $listref = shift;
    logger($self->{_debug}, "Entering Environment_obj::getListenerName",1);
    my $envlisteners = $self->{_envlisteners};
    
    my $ret;
    
    #my $listref = { $envlisteners->{$envref}->{$_}->{reference} eq $listref } keys %{$envlisteners->{$envref}};
    
    if (defined($envlisteners->{$envref}->{$listref})) {
      $ret = $envlisteners->{$envref}->{$listref}->{name};
    } else {
      $ret = 'N/A';
    }
    
    return $ret;
  
}



#procedure getAllEnvironmentListenersPorts
#parameters: none
#return array of listener ports

sub getAllEnvironmentListenersPorts {
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::getAllEnvironmentListenersPorts",1);

    my @ports;

    my $envlisteners = $self->{_envlisteners};

    for my $env (sort keys %{$envlisteners}) {
        push (@ports, $self->getEnvironmentListenerPorts( $env ));
    }

    my %portshash   = map { $_, 1 } @ports;

    return \%portshash;
}


# Procedure getEnvironmentListeners
# parameters: none
# Load a list of listeners for all environment objects from Delphix Engine

sub getEnvironmentListeners
{
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentListeners",1);

    my $operation = "resources/json/delphix/environment/oracle/listener";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $envlisteners = $self->{_envlisteners};
        for my $envlist (@res) {
            $envlisteners->{$envlist->{environment}}->{$envlist->{reference}} = $envlist;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
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

    logger($self->{_debug}, "Entering Environment_obj::runJobOperation",1);
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
            print "Problem with job " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    return $jobno;
}


# Procedure createEnv
# parameters:
# - type - environment type (unix/windows)
# - name - envoironment name
# - host - environment host
# - toolkit_path - toolkit path
# - username - host username
# - authtype - user auth type (system key / password)
# - password
# - proxy
# start create job
# Return job name is sucessful or undef

sub createEnv
{
    my $self = shift;
    my $type = shift;
    my $name = shift;
    my $host = shift;
    my $toolkit_path = shift;
    my $username = shift;
    my $authtype = shift;
    my $password = shift;
    my $proxy = shift;
    logger($self->{_debug}, "Entering Environment_obj::createEnv",1);

    my @addr;

    push (@addr, $host);

    my $operation = "resources/json/delphix/environment";
    my %env = (
        "type" => "HostEnvironmentCreateParameters",
        "primaryUser" => {
            "type" => "EnvironmentUser",
            "name" => $username
        },
        "hostEnvironment" => {
            "name" => $name
        },
        "hostParameters" => {
            "host" => {
                "address" => $host
            }
        }

    );

    # {
    #     "type": "WindowsClusterCreateParameters",
    #     "primaryUser": {
    #         "type": "EnvironmentUser",
    #         "name": "DELPHIX\\delphix_admin",
    #         "credential": {
    #             "type": "PasswordCredential",
    #             "password": "delphix"
    #         }
    #     },
    #     "cluster": {
    #         "type": "WindowsCluster",
    #         "name": "CLU2012",
    #         "address": "192.168.1.170",
    #         "proxy": "WINDOWS_HOST-266774"
    #     }
    # }

    if ($type eq 'unix') {
        $env{"hostEnvironment"}{"type"} = "UnixHostEnvironment";
        $env{"hostParameters"}{"host"}{"type"} = "UnixHost";
        $env{"hostParameters"}{"type"} = "UnixHostCreateParameters";
        $env{"hostParameters"}{"host"}{"toolkitPath"} = $toolkit_path;
        #$env{"hostParameters"}{"host"}{"addresses"} = \@addr;
        #$env{"hostParameters"}{"host"}{"address"} = $host;
    } elsif ($type eq 'windows') {
        $env{"hostEnvironment"}{"type"} = "WindowsHostEnvironment";
        $env{"hostParameters"}{"host"}{"type"} = "WindowsHost";
        #$env{"hostParameters"}{"host"}{"address"} = $host;
        $env{"hostParameters"}{"type"} = "WindowsHostCreateParameters";
        if (defined($toolkit_path)) {
          $env{"hostParameters"}{"host"}{"toolkitPath"} = $toolkit_path;
        }
        if (defined($proxy)) {
          $env{"hostEnvironment"}{"proxy"} = $proxy;
        }
    } else {
        return undef;
    }

    my %cred;

    if ($authtype eq 'systemkey') {
        %cred = (
            "type" => "SystemKeyCredential"
        );

    }
    elsif ($authtype eq 'password') {
        %cred = (
            "type" => "PasswordCredential",
            "password" => $password
        );

    } else {
        return undef;
    }


    $env{"primaryUser"}{"credential"} = \%cred;

    my $json_data = encode_json(\%env);

    return $self->runJobOperation($operation, $json_data);
}




# Procedure refresh
# parameters:
# - reference - environment reference
# start refresh job
# Return job name is sucessful or undef

sub refresh
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Environment_obj::refresh",1);


    my $operation = "resources/json/delphix/environment/" . $reference . "/refresh";

    return $self->runJobOperation($operation, "{}");
}


# Procedure disable
# parameters:
# - reference - environment reference
# start disable job
# Return 0 if OK and 1 if there was a problem

sub disable
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Environment_obj::disable",1);


    my $operation = "resources/json/delphix/environment/" . $reference . "/disable";

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, "{}");

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        return 0;
    } else {
        return 1;
    }

}

# Procedure enable
# parameters:
# - reference - environment reference
# start enable job
# Return job name is sucessful or undef

sub enable
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Environment_obj::enable",1);


    my $operation = "resources/json/delphix/environment/" . $reference . "/enable";

    return $self->runJobOperation($operation, "{}");
}


# Procedure changePassword
# parameters:
# - reference - user reference
# - password - password
# start enable job
# Return job name is sucessful or undef

sub changePassword
{
    my $self = shift;
    my $reference = shift;
    my $password = shift;
    logger($self->{_debug}, "Entering Environment_obj::changePassword",1);


    my $operation = "resources/json/delphix/environment/user/" . $reference;
    my %pass_data = (
        "type" => "EnvironmentUser",
        "credential" => {
            "type" =>  "PasswordCredential",
            "password" => $password
        }
    );

    my $json_data = to_json(\%pass_data, {pretty=>1});

    return $self->runJobOperation($operation, $json_data, 'ACTION');
}

1;
