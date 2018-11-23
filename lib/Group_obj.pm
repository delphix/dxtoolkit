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
# Program Name : Group_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - Group_obj - class which map a Delphix Engine group API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#


package Group_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Encode qw(decode_utf8);


sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Group_obj::constructor",1);

    my %groups;
    my $self = {
        _groups => \%groups,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    my $namespace = new Namespace_obj ( $dlpxObject, $debug );

    $self->{_namespace} = $namespace;

    bless($self,$classname);

    $self->loadGroupsList($debug);
    return $self;
}

# Procedure getGroupByName
# parameters:
# - name
# Return group reference for specific group name

sub getGroupByName {
    my $self = shift;
    my $name = shift;
    my $ret;

    $name = decode_utf8($name);

    logger($self->{_debug}, "Entering Group_obj::getGroupByName",1);

    for my $groupitem ( sort ( keys %{$self->{_groups}} ) ) {
        if ( $self->getName($groupitem) eq $name) {
            $ret = $self->getGroup($groupitem);
        }
    }

    return $ret;
}

# Procedure getGroup
# parameters:
# - reference
# Return group hash for specific group reference

sub getGroup {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Group_obj::getGroup",1);

    my $groups = $self->{_groups};
    return $groups->{$reference};
}

# Procedure getName
# parameters:
# - reference
# Return group name for specific group reference

sub getName {
   my $self = shift;
   my $reference = shift;

   logger($self->{_debug}, "Entering Group_obj::getName",1);

   my $groups = $self->{_groups};
   my $ret;


   if (defined($groups->{$reference}->{name}) ) {
     if (defined($groups->{$reference}->{namespace})) {
       my $namespacename = $self->{_namespace}->getName($groups->{$reference}->{namespace});
       $ret = $groups->{$reference}->{name} . "@" . $namespacename;
     } else {
       $ret = $groups->{$reference}->{name};
     }
   } else {
      $ret = 'N/A';
   }

   return $ret;
}

# Procedure getGroupList
# parameters:
# Return list of group references

sub getGroupList {
    my $self = shift;

    logger($self->{_debug}, "Entering Group_obj::getGroupList",1);

    my $groups = $self->{_groups};
    my @sortedgroups = sort { $self->getName($a) cmp $self->getName($b) } ( keys %{$groups} );
    return \@sortedgroups;
}

# Procedure getPrimaryGroupList
# parameters:
# Return list of primary group references

sub getPrimaryGroupList {
    my $self = shift;

    logger($self->{_debug}, "Entering Group_obj::getPrimaryGroupList",1);

    my $groups = $self->{_groups};
    my @sortedgroups = sort { $self->getName($a) cmp $self->getName($b) } ( keys %{$groups} );

    my @ret = grep { ! defined($groups->{$_}->{namespace}) } @sortedgroups;

    return \@ret;
}


# Procedure loadGroupsList
# parameters: none
# Load a list of groups objects from Delphix Engine

sub loadGroupsList
{
    my $self = shift;

    logger($self->{_debug}, "Entering Group_obj::loadGroupsList",1);
    my $operation = "resources/json/delphix/group";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $groups = $self->{_groups};
        for my $groupitem (@res) {
            $groups->{$groupitem->{reference}} = $groupitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}


# Procedure createGroup
# parameters:
# - name - group name
# Create a group name
# return action

sub createGroup {
    my $self = shift;
    my $name = shift;

    logger($self->{_debug}, "Entering Group_obj::createGroup",1);

    my $operation = 'resources/json/delphix/group';
    my %hash_group = (
      "type" => "Group",
      "name" => $name
    );

    my $json_data = to_json(\%hash_group);

    return $self->runJobOperation($operation, $json_data, 'ACTION');
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

    logger($self->{_debug}, "Entering Network_obj::runJobOperation",1);
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

1;
