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
    $self->listEnvironmentUsers();
    $self->getEnvironmentListeners();
    return $self;
}


# Procedure getAllEnvironments
# parameters: none
# Return list of environments (references)

sub getAllEnvironments {
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::getAllEnvironments",1);


    my @mainenv = grep { ($self->{_environments}->{$_}->{type} ne 'OracleClusterNode') && ($self->{_environments}->{$_}->{type} ne 'WindowsClusterNode') } keys %{$self->{_environments}};

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


# Procedure getEnvironmentUsers
# parameters:
# - reference
# Return environment users for environment


sub getEnvironmentUsers {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUsers",1);

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


# Procedure getEnvironmentUserAuth
# parameters:
# - environmet ref
# - user ref
# Return environment user auth for environment and user


sub getEnvironmentUserAuth {
    my $self = shift;
    my $reference = shift;
    my $userref = shift;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUserAuth",1);

    my $envusers = $self->{_envusers};
    my $ret;

    if ( defined($envusers->{$reference}) && ( defined($envusers->{$reference}->{$userref})  ) ) {
      my $auth = $envusers->{$reference}->{$userref}->{credential}->{type};
      if ($auth eq 'PasswordCredential') {
        $ret = 'password';
      } elsif ($auth eq 'KeyPairCredential') {
        $ret = 'systemkey';
      } elsif ($auth eq 'KerberosCredential') {
        $ret = 'kerberos';
      }
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

    return $self->getEnvironmentUserAuth($reference, $self->getPrimaryUser($reference));

    # my $environments = $self->{_environments};
    #
    # #my $username = $self->{_envusers}->{name};
    #
    # my $ret = $environments->{$reference}->{_primaryUserAuth};
    # if ($ret eq 'PasswordCredential') {
    #   $ret = 'password';
    # } elsif ($ret eq 'KeyPairCredential') {
    #   $ret = 'systemkey';
    # }
    #
    # return $ret;
}

# Procedure getEnvironmentNotPrimaryUsers
# parameters:
# - reference
# Return environment user array without primary user

sub getEnvironmentNotPrimaryUsers {
    my $self = shift;
    my $envitem = shift;
    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentNotPrimaryUsers",1);

    my $primaryUser = $self->getPrimaryUser($envitem);
    my @users_withoutprim = grep { $_ ne $primaryUser  } keys %{$self->{_envusers}->{$envitem}};

    return \@users_withoutprim;

}


# Procedure getConfig
# parameters:
# - reference
# Return environment metadata

sub getConfig {
    my $self = shift;
    my $envitem = shift;
    my $host_obj = shift;
    my $backup = shift;

    logger($self->{_debug}, "Entering Environment_obj::getConfig",1);

    my $config = '';
    my $joinsep;

    if (defined($backup)) {
      $joinsep = ' ';
    } else {
      $joinsep = ',';
    }

    my $envtype = $self->getType($envitem);
    my $host_ref = $self->getHost($envitem);
    if ($envtype eq 'rac') {
      my $clusenvnode = $self->getClusterNode($envitem);
      $host_ref = $self->getHost($clusenvnode);
    }

    my $toolkit = $host_obj->getToolkitpath($host_ref);
    if (!defined($toolkit)) {
      $toolkit = 'N/A';
    }
    my $proxy_ref = $self->getProxy($envitem);
    my $proxy;
    if ($proxy_ref eq 'N/A') {
      $proxy = 'N/A';
    } else {
      $proxy = $host_obj->getHostAddr($proxy_ref);
    }

    if ($toolkit eq 'N/A') {
      $config = join($joinsep,($config, "-proxy $proxy"));
    } else {
      $config = join($joinsep,($config, "-toolkitdir \"$toolkit\""));
    }

    if ($envtype eq 'rac') {
      my $clusloc = $self->getClusterloc($envitem);
      my $clustname = $self->getClusterName($envitem);
      $config = join($joinsep,($config, "-clusterloc $clusloc -clustername $clustname "));
    }

    my $asedbuser =  $self->getASEUser($envitem);
    if ($asedbuser ne 'N/A') {
      $config = join($joinsep,($config, "-asedbuser $asedbuser -asedbpass ChangeMeDB"));
    }

    my $rest;

    if ( ( $rest = $config ) =~ /^,(.*)/ )   {
      $config = $1;
    }

    return $config;

}



# Procedure getBackup
# parameters:
# - reference
# Return environment metadata backup

sub getBackup {
    my $self = shift;
    my $envitem = shift;
    my $host_obj = shift;
    my $engine = shift;
    my $envname = shift;
    my $envtype = shift;
    my $hostname = shift;
    my $user = shift;
    my $userauth = shift;

    logger($self->{_debug}, "Entering Environment_obj::getBackup",1);

    my $suffix = '';
    if ( $^O eq 'MSWin32' ) {
      $suffix = '.exe';
    }

    my $backup = "dx_create_env$suffix -d $engine -envname $envname -envtype $envtype -host $hostname -username \"$user\" -authtype $userauth -password ChangeMe ";
    $backup = $backup . $self->getConfig($envitem, $host_obj, 1);

    return $backup;

}

# Procedure getUserBackup
# parameters:
# - reference
# Return environment metadata backup

sub getUsersBackup {
   my $self = shift;
   my $envitem = shift;
   my $output = shift;
   my $engine = shift;

   my $backup;

   logger($self->{_debug}, "Entering Environment_obj::getUserBackup",1);

   my $suffix = '';
   if ( $^O eq 'MSWin32' ) {
     $suffix = '.exe';
   }

   my $name = $self->getName($envitem);
   my $auth;

   for my $useritem (@{$self->getEnvironmentNotPrimaryUsers($envitem)}) {

     $backup = "dx_ctl_env$suffix -d $engine -envname " . $name . " -action adduser -username \"" .$self->getEnvironmentUserNamebyRef($envitem,$useritem) . "\"";
     $auth = $self->getEnvironmentUserAuth($envitem,$useritem);
     if ($auth eq 'password') {
       $backup = $backup . " -authtype password -password ChangeMe";
     } else {
       $backup = $backup . " -authtype systemkey";
     }

     $output->addLine(
      $backup
     );
   }

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
    } elsif ($ret eq 'WindowsCluster') {
      $ret = 'windows-cluster';
    }elsif ($ret eq 'OracleCluster') {
      $ret = 'rac';
    }

    return $ret;
}

# Procedure getClusterloc
# parameters:
# - reference
# Return environment cluster location for specific environment reference

sub getClusterloc {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getClusterloc",1);

    my $environments = $self->{_environments};
    my $ret;
    if ($environments->{$reference}->{'type'} eq 'OracleCluster') {
      $ret = $environments->{$reference}->{crsClusterHome};
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure getClusterName
# parameters:
# - reference
# Return environment cluster name for specific environment reference

sub getClusterName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getClusterName",1);

    my $environments = $self->{_environments};
    my $ret;
    if ($environments->{$reference}->{'type'} eq 'OracleCluster') {
      $ret = $environments->{$reference}->{crsClusterName};
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure getClusterNode
# parameters:
# - reference
# Return environment cluster location for specific environment reference

sub getClusterNode {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getClusterNode",1);

    my $environments = $self->{_environments};
    my $ret;

    if (($environments->{$reference}->{'type'} eq 'OracleCluster') || ($environments->{$reference}->{'type'} eq 'WindowsCluster')) {
      my @nodes = grep { defined($environments->{$_}->{cluster}) && ( $environments->{$_}->{cluster} eq $reference ) } sort (keys %{$environments} );
      $ret = $nodes[0];
    } else {
      $ret = 'N/A';
    }

    return $ret;
}

# Procedure getASEUser
# parameters:
# - reference
# Return environment ASE user

sub getASEUser {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Environment_obj::getASEUser",1);

    my $environments = $self->{_environments};
    my $ret;
    if ($environments->{$reference}->{'type'} eq 'UnixHostEnvironment') {
      if (defined($environments->{$reference}->{aseHostEnvironmentParameters})) {
        $ret = $environments->{$reference}->{aseHostEnvironmentParameters}->{dbUser};
      } else {
        $ret = 'N/A';
      }
    } else {
      $ret = 'N/A';
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
# Return environment user name for environment name and user ref

sub getEnvironmentUserNamebyRef {
    my $self = shift;
    my $ref = shift;
    my $user = shift;
    my $ret;

    logger($self->{_debug}, "Entering Environment_obj::getEnvironmentUserByRef",1);

    if (defined($self->{_environments}->{$ref})) {
        # is this a environment refrerence
        my $users = $self->getEnvironmentUsers($ref);
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
        my $users = $self->getEnvironmentUsers($name);
        logger($self->{_debug}, "Environment ref ". Dumper $name , 2);
        logger($self->{_debug}, "Environment users ". Dumper $users , 2);
        logger($self->{_debug}, "Looking for user ". Dumper $username , 2);
        my @t = grep { $users->{$_}->{name} eq $username } keys %{$users};
        logger($self->{_debug}, "matching users ". Dumper \@t , 2);
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
                my $users = $self->getEnvironmentUsers($envitem);
                logger($self->{_debug}, "Environment name ". Dumper $name , 2);
                logger($self->{_debug}, "Environment users ". Dumper $users , 2);
                logger($self->{_debug}, "Looking for user ". Dumper $username , 2);
                my @t = grep { $users->{$_}->{name} eq $username } keys %{$users};
                logger($self->{_debug}, "matching users ". Dumper \@t , 2);
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

    $operation = "resources/json/delphix/environment/windows/clusternode";
    ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $environments = $self->{_environments};
        for my $envitem (@res) {
            $environments->{$envitem->{reference}} = $envitem;
        }

    }



}

# Procedure listEnvironmentUsers
# parameters: none
# Load a list of users for all environment objects from Delphix Engine

sub listEnvironmentUsers
{
    my $self = shift;
    logger($self->{_debug}, "Entering Environment_obj::listEnvironmentUsers",1);

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
# - crsname
# - crsloc
# - sshport
# - ASE db user
# - ASE db password
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
    my $crsname = shift;
    my $crsloc = shift;
    my $port = shift;
    my $asedbuser = shift;
    my $asedbpass = shift;
    logger($self->{_debug}, "Entering Environment_obj::createEnv",1);

    my @addr;

    push (@addr, $host);

    my $operation = "resources/json/delphix/environment";
    my %env = (
        "primaryUser" => {
            "type" => "EnvironmentUser",
            "name" => $username
        }
    );


    my %host;
    if (($type eq 'unix') || ($type eq 'rac')) {

      if (!defined($port)) {
        $port = 22;
      }

      %host = (
        "type" => "UnixHostCreateParameters",
        "host" => {
            "type" => "UnixHost",
            "address" => $host,
            "sshPort" => $port,
            "toolkitPath" => $toolkit_path
        }
      );
    } elsif ($type eq 'windows') {

      if (!defined($port)) {
        $port = 9100;
      }

      %host = (
        "type" => "WindowsHostCreateParameters",
        "host" => {
            "type" => "WindowsHost",
            "connectorPort" => $port,
            "address" => $host
        }
      );

      if (defined($toolkit_path)) {
        $host{"host"}{"toolkitPath"} = $toolkit_path;
      }
      if (defined($proxy)) {
        delete $host{"host"}{"connectorPort"};
      }
    }


    if ($type eq 'unix') {
        $env{"type"} = "HostEnvironmentCreateParameters";
        $env{"hostEnvironment"}{"type"} = "UnixHostEnvironment";
        $env{"hostEnvironment"}{"name"} = $name;
        $env{"hostParameters"} = \%host;

        if (defined($asedbuser) )  {

          $env{"hostEnvironment"}{"aseHostEnvironmentParameters"}{"type"} = "ASEHostEnvironmentParameters";
          $env{"hostEnvironment"}{"aseHostEnvironmentParameters"}{"dbUser"} = $asedbuser;
          $env{"hostEnvironment"}{"aseHostEnvironmentParameters"}{"credentials"}{"type"} = "PasswordCredential";
          $env{"hostEnvironment"}{"aseHostEnvironmentParameters"}{"credentials"}{"password"} = $asedbpass;

        }

    } elsif ($type eq 'windows') {
        $env{"type"} = "HostEnvironmentCreateParameters";
        $env{"hostEnvironment"}{"type"} = "WindowsHostEnvironment";
        $env{"hostEnvironment"}{"name"} = $name;

        if (defined($proxy)) {
          $env{"hostEnvironment"}{"proxy"} = $proxy;
        }

        $env{"hostParameters"} = \%host;
    } elsif ($type eq 'rac') {
      $env{"type"} = "OracleClusterCreateParameters";
      $env{"cluster"}{"type"} = "OracleCluster";
      $env{"cluster"}{"crsClusterName"} = $crsname;
      $env{"cluster"}{"crsClusterHome"} = $crsloc;

      my @nodes;

      my %node1 = (
        "type" => "OracleClusterNodeCreateParameters",
        "hostParameters" => \%host
      );

      push (@nodes, \%node1);
      $env{"nodes"} = \@nodes;

    } elsif ($type eq 'wincluster') {
      $env{"type"} = "WindowsClusterCreateParameters";
      $env{"cluster"}{"type"} = "WindowsCluster";
      $env{"cluster"}{"name"} = $name;
      $env{"cluster"}{"proxy"} = $proxy;
      $env{"cluster"}{"address"} = $host;
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


# Procedure delete
# parameters:
# - reference - environment reference
# Delete environment
# Return job name is sucessful or undef

sub delete
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Environment_obj::delete",1);


    my $operation = "resources/json/delphix/environment/" . $reference . "/delete";

    return $self->runJobOperation($operation, "{}");
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


# Procedure changeASEPassword
# parameters:
# - reference - env reference
# - password - password
# start enable job
# Return job name is sucessful or undef

sub changeASEPassword
{
    my $self = shift;
    my $reference = shift;
    my $password = shift;
    logger($self->{_debug}, "Entering Environment_obj::changeASEPassword",1);


    my $operation = "resources/json/delphix/environment/" . $reference;
    my %pass_data = (
        "type" => "UnixHostEnvironment",
        "aseHostEnvironmentParameters" => {
            type => "ASEHostEnvironmentParameters",
            "credentials" => {
              "password" => $password,
              "type" => "PasswordCredential"
            }
          }
    );

    my $json_data = to_json(\%pass_data, {pretty=>1});

    return $self->runJobOperation($operation, $json_data, 'ACTION');
}

# Procedure createEnvUser
# parameters:
# - reference - env reference
# - username
# - authtype
# - password - password
# Create environment user
# Return 0 if OK

sub createEnvUser
{
    my $self = shift;
    my $reference = shift;
    my $username = shift;
    my $authtype = shift;
    my $password = shift;
    logger($self->{_debug}, "Entering Environment_obj::createEnvUser",1);


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

    }

    my $operation = "resources/json/delphix/environment/user/";
    my %pass_data = (
        "type" => "EnvironmentUser",
        "credential" => \%cred,
        "name" => $username,
        "environment" => $reference
    );

    my $json_data = to_json(\%pass_data, {pretty=>1});
    logger($self->{_debug}, $json_data, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      print "User $username created \n";
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with user creation " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
}


# Procedure deleteEnvUser
# parameters:
# - reference - env reference
# - username - username
# Delete environent user
# Return 0 if OK

sub deleteEnvUser
{
    my $self = shift;
    my $reference = shift;
    my $username = shift;
    logger($self->{_debug}, "Entering Environment_obj::deleteEnvUser",1);


    my $userref = $self->getEnvironmentUserByName($reference, $username);

    if (!defined($userref)) {
      print "Username $username not found \n";
      return 1;
    }

    my $operation = "resources/json/delphix/environment/user/" . $userref ."/delete";

    my %del_data = (
      "type" => "DeleteParameters"
    );

    my $json_data = to_json(\%del_data, {pretty=>1});
    logger($self->{_debug}, $json_data, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      print "User $username deleted \n";
      $ret = 0;
    } else {
        $ret = 1;
        if (defined($result->{error})) {
            print $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
    return $ret;
}


# Procedure createListener
# parameters:
# - reference - env reference
# - listenername - name of listener
# - endpoint - array of end points
# Create listener
# Return 0 if OK

sub createListener
{
    my $self = shift;
    my $reference = shift;
    my $listenername = shift;
    my $endpoint = shift;
    logger($self->{_debug}, "Entering Environment_obj::createListener",1);


    my $operation = "resources/json/delphix/environment/oracle/listener";
    my $env = $self->getEnvironment($reference);

    if (!defined($env->{host})) {
      print "RAC environment is not supported now\n";
      return 1;
    }

    my @badendpoint = grep { scalar(split(':',$_)) ne 2 } @{$endpoint};

    if (scalar(@badendpoint)>0) {
      print "Endpoint definition doesn't match hostname:port \n";
      return 1;
    }


    my %listener_hash = (
        "type" => "OracleNodeListener",
        "name" => $listenername,
        "endPoints" => $endpoint,
        "environment" => $reference,
        "host" => $env->{host}
    );

    my $json_data = to_json(\%listener_hash, {pretty=>1});
    logger($self->{_debug}, $json_data, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      print "Listener $listenername created \n";
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with listener creation " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
}



# Procedure deleteListener
# parameters:
# - reference - env reference
# - listenername

# Delete listener by name
# Return 0 if OK

sub deleteListener
{
    my $self = shift;
    my $reference = shift;
    my $listenername = shift;
    logger($self->{_debug}, "Entering Environment_obj::deleteListener",1);

    my $listref = $self->getListenerByName($reference,$listenername);

    if (!defined($listref)) {
      print "Listener $listenername not found\n";
      return 1;
    }

    my $operation = "resources/json/delphix/environment/oracle/listener/" . $listref . "/delete";

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, '{}');
    my $ret;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      print "Listener $listenername deleted \n";
      $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with listener deletion " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
}

1;
