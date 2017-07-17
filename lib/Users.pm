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
# Program Name : Users.pm
# Description  : Delphix Engine User object
# It's include the following classes:
# - Users - class which map a Delphix Engine user API object
# Author       : Marcin Przepiorowski
# Created      : 24 Apr 2015 (v2.0.0)
#
#

package Users;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use User_obj;


# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $databases = shift;
    my $debug = shift;
    logger($debug, "Entering Users::constructor",1);

    my %users;
    my $self = {
        _users => \%users,
        _dlpxObject => $dlpxObject,
        _databases => $databases,
        _debug => $debug
    };
    
    bless($self,$classname);

    my $authorizations = new Authorization_obj($dlpxObject,$debug);
    
    $self->{_authorizations} = $authorizations;

    $self->getUserList($debug);
    return $self;
}


# Procedure getUserByName
# parameters: 
# - name 
# Return user reference for particular user name

sub getUserByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Users::getUserByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $useritem ( sort ( keys %{$self->{_users}} ) ) {
        my $user = $self->{_users}->{$useritem};
        if ( $user->getName() eq $name) {
            $ret = $user;
        }
    }

    return $ret;
}

# Procedure getUser
# parameters: 
# - reference
# Return user hash for specific user reference

sub getUser {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Users::getUser",1);    

    my $users = $self->{_users};
    return $users->{$reference};

}

# Procedure getUsers
# parameters: 
# Return list of users

sub getUsers {
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getUsers",1); 
    return sort (keys %{$self->{_users}});
}


# Procedure getJSUsers
# parameters: 
# Return list of JS users plus delphix admin one as they can have JS objects

sub getJSUsers {
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getJSUsers",1); 
    my @retarray;
    for my $userref (sort (keys %{$self->{_users}})) {
      if (($self->{_users}->{$userref}->isJS()) || ($self->{_users}->{$userref}->isAdmin())) {
        push(@retarray, $userref);
      }
    }
    return \@retarray;
}


# Procedure getUsersByTarget
# parameters: 
# - target ref
# Return list of users for target

sub getUsersByTarget {
    my $self = shift;
    my $target_ref = shift;
    logger($self->{_debug}, "Entering Users::getUsersByTarget",1); 
    
    return $self->{_authorizations}->getUsersByTarget($target_ref);
}

# Procedure getUserList
# parameters: none
# Load a list of user objects from Delphix Engine

sub getUserList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Users::getUserList",1);   
    
    my $databases;
    if (defined($self->{_databases})) {
      $databases = $self->{_databases};
    } else {
      $databases = new Databases($self->{_dlpxObject},$self->{_debug});
      $self->{_databases} = $databases;
    }

    my $operation = "resources/json/delphix/user";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};


        for my $useritem (@res) {
            my $user = new User_obj($self->{_dlpxObject}, $self, $self->{_debug});
            $user->{_databases} = $databases;
            $user->{_user} = $useritem;
            $self->{_users}->{$useritem->{reference}} = $user;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;