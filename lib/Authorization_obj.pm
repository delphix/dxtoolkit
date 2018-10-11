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
# Program Name : Authorization_obj.pm
# Description  : Delphix Engine authorization object
# It's include the following classes:
# - Authorization_obj - class which map a Delphix Engine authorization API object
# Author       : Marcin Przepiorowski
# Created      : 24 Apr 2015 (v2.0.0)
#
#


package Authorization_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Roles_obj;
use version;

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Authorization_obj::constructor",1);

    my $roles = new Roles_obj($dlpxObject,$debug);

    my %authorizations;
    my $self = {
        _authorizations => \%authorizations,
        _dlpxObject => $dlpxObject,
        _roles => $roles,
        _debug => $debug
    };

    bless($self,$classname);

    $self->getAuthorizationList($debug);
    return $self;
}


# Procedure getRoleByName
# parameters:
# - user_ref
# Return authorization reference array for particular user ref

sub getAuthorizationByUser {
    my $self = shift;
    my $user_ref = shift;
    logger($self->{_debug}, "Entering Authorization_obj::getAuthorizationByUser",1);
    my @ret;

    #print Dumper $$config;

    for my $authitem ( sort ( keys %{$self->{_authorizations}} ) ) {

        if ( $self->getUser($authitem) eq $user_ref) {
            push (@ret, $self->getAuthotization($authitem));
        }
    }

    return \@ret;
}

# Procedure getUsersByTarget
# parameters:
# - target_ref
# Return array of hash ( user / role name) for particular target ref


sub getUsersByTarget {
    my $self = shift;
    my $target_ref = shift;
    logger($self->{_debug}, "Entering Authorization_obj::getUsersByTarget",1);

    my @retarray;

    for my $authitem ( sort ( keys %{$self->{_authorizations}} ) ) {

      if ( $self->getTarget($authitem) eq $target_ref) {
        push(@retarray, $self->getUser($authitem));
      }

    }

    return \@retarray;

}


# Procedure getDatabasesByUser
# parameters:
# - user_ref
# Return array of hash ( database / role name) for particular user ref
# but limited to database objects only

sub getDatabasesByUser {
    my $self = shift;
    my $user_ref = shift;
    logger($self->{_debug}, "Entering Authorization_obj::getAuthorizationByUser",1);
    my %db_hash;
    my @retarray;

    #print Dumper $$config;

    for my $authitem ( sort ( keys %{$self->{_authorizations}} ) ) {

        if ( $self->getUser($authitem) eq $user_ref) {
            if ( $self->isDatabaseObject($authitem) ) {
                my $local_auth = $self->getAuthotization($authitem);
                my %db_hash;
                $db_hash{'obj_ref'} = $local_auth->{target};
                $db_hash{'name'} = $self->{_roles}->getName($local_auth->{role});
                $db_hash{'authref'} = $local_auth->{reference};
                push(@retarray, \%db_hash);
            }
        }
    }

    return \@retarray;
}

# Procedure getAuthotization
# parameters:
# - reference
# Return authotization hash for specific authotization reference

sub getAuthotization {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Authorization_obj::getAuthotization",1);

    my $authorizations = $self->{_authorizations};
    return $authorizations->{$reference}
}


# Procedure isDatabaseObject
# parameters:
# - reference
# Return 1 for database objects and 0 for non database objects

sub isDatabaseObject {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Authorization_obj::isDatabaseObject",1);

    my $authorizations = $self->{_authorizations};

    my $target = $authorizations->{$reference}->{target};

    if (( $target eq 'DOMAIN' ) || ( $target =~ /USER/ )) {
        return 0
    } else {
        return 1;
    }
}

# Procedure isEngineAdmin
# parameters:
# - user_ref
# Return authorization ref if is Admin

sub isEngineAdmin {
    my $self = shift;
    my $user_ref = shift;
    my $ret;

    logger($self->{_debug}, "Entering Authorization_obj::isEngineAdmin",1);

    my $authorizations = $self->{_authorizations};

    my $admin_role = $self->{_roles}->getRoleByName('OWNER')->{reference};

    for my $authitem ( sort ( keys %{$self->{_authorizations}} ) ) {
        if ( $self->getUser($authitem) eq $user_ref) {
            my $target = $authorizations->{$authitem}->{target};
            my $role = $authorizations->{$authitem}->{role};
            if (( $target eq 'DOMAIN' ) && ( $role eq $admin_role )) {
                $ret = $authitem;
            }
        }
    }

    return $ret;

}

# Procedure isJS
# parameters:
# - user_ref
# Return authorization ref if is Admin

sub isJS {
    my $self = shift;
    my $user_ref = shift;
    my $ret;

    logger($self->{_debug}, "Entering Authorization_obj::isJS",1);

    my $authorizations = $self->{_authorizations};

    my $jsuser;
    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse("1.10.0")) {
      $jsuser = $self->{_roles}->getRoleByName('Jet Stream User')->{reference};
    } else {
      $jsuser = $self->{_roles}->getRoleByName('Self-Service User')->{reference};
    }

    for my $authitem ( sort ( keys %{$self->{_authorizations}} ) ) {
        if ( $self->getUser($authitem) eq $user_ref) {
            my $target = $authorizations->{$authitem}->{target};
            my $role = $authorizations->{$authitem}->{role};
            if (( $target eq $user_ref ) && ( $role eq $jsuser )) {
                $ret = $authitem;
            }
        }
    }

    return $ret;

}

# Procedure getTarget
# parameters:
# - reference
# Return authorization target ref for specific authorization reference

sub getTarget {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Authorization_obj::getTarget",1);

    my $authorizations = $self->{_authorizations};
    return $authorizations->{$reference}->{target};
}

# Procedure getUser
# parameters:
# - reference
# Return authorization user ref for specific authorization reference

sub getUser {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Authorization_obj::getUser",1);

    my $authorizations = $self->{_authorizations};
    return $authorizations->{$reference}->{user};
}

# Procedure setAuthorisation
# parameters:
# - user ref
# - role name
# - target ref
# Return 0 if OK

sub setAuthorisation {
    my $self = shift;
    my $user_ref = shift;
    my $role_name = shift;
    my $target_ref = shift;

    logger($self->{_debug}, "Entering Authorization_obj::setAuthorisation",1);

    my $operation = "resources/json/delphix/authorization";
    logger($self->{_debug}, $operation, 2);

    my $roleobj = $self->{_roles}->getRoleByName($role_name);
    if (!defined($roleobj)) {
      print "Role $role_name not found. ";
      return 1;
    }

    my $role_ref = $self->{_roles}->getRoleByName($role_name)->{reference};

    my %auth = (
        type => "Authorization",
        role => $role_ref,
        user => $user_ref,
        target => $target_ref
    );

    my $json_data = to_json(\%auth);

    #print Dumper $json_data;

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    #print Dumper $result_fmt;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $self->getAuthorizationList($self->{_debug});
        return 0;
    } else {
        return 1;
    }

}

# Procedure deleteAuthorisation
# parameters:
# - reference
# Delete authorization
# Return 0 if OK

sub deleteAuthorisation {
    my $self = shift;
    my $reference = shift;


    logger($self->{_debug}, "Entering Authorization_obj::deleteAuthorisation",1);

    my $operation = "resources/json/delphix/authorization/" . $reference . "/delete";
    logger($self->{_debug}, $operation, 2);

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, "{}");

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        delete $self->{_authorizations}->{$reference};
        return 0;
    } else {
        return 1;
    }

}


# Procedure getAuthorizationList
# parameters: none
# Load a list of authorization objects from Delphix Engine

sub getAuthorizationList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Authorization_obj::getRolesList",1);

    my $operation = "resources/json/delphix/authorization";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $authorizations = $self->{_authorizations};

        for my $authitem (@res) {
            $authorizations->{$authitem->{reference}} = $authitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;
