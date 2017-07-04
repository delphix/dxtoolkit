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
# Program Name : Roles_obj.pm
# Description  : Delphix Engine Role object
# It's include the following classes:
# - Roles_obj - class which map a Delphix Engine role API object
# Author       : Marcin Przepiorowski
# Created      : 24 Apr 2015 (v2.0.0)
#
#


package Roles_obj;

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
    logger($debug, "Entering Role_obj::constructor",1);

    my %roles;
    my $self = {
        _roles => \%roles,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->getRolesList($debug);
    return $self;
}


# Procedure getRoleByName
# parameters: 
# - name 
# Return role reference for particular name

sub getRoleByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Role_obj::getRoleByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $roleitem ( sort ( keys %{$self->{_roles}} ) ) {

        if ( lc $self->getName($roleitem) eq lc $name) {
            $ret = $self->getRole($roleitem); 
        }
    }

    return $ret;
}

# Procedure getRole
# parameters: 
# - reference
# Return role hash for specific role reference

sub getRole {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Role_obj::getRole",1);    

    my $roles = $self->{_roles};
    return $roles->{$reference}
}

# Procedure getName
# parameters: 
# - reference
# Return role name for specific role reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Role_obj::getName",1);   

    my $roles = $self->{_roles};
    return $roles->{$reference}->{name};
}

# Procedure getRolesList
# parameters: none
# Load a list of role objects from Delphix Engine

sub getRolesList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Role_obj::getRolesList",1);   

    my $operation = "resources/json/delphix/role";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $roles = $self->{_roles};

        for my $roleitem (@res) {
            $roles->{$roleitem->{reference}} = $roleitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;