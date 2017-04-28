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
# Program Name : Action_obj.pm
# Description  : Delphix Engine Action object
# It's include the following classes:
# - Action_obj - class which map a Delphix Engine action API object
# Author       : Marcin Przepiorowski
# Created      : 20 Jul 2015 (v2.0.X)
#
#


package Action_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Date::Manip;
use lib '../lib';
use Jobs_obj;


# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $startTime = shift;
    my $endTime = shift;
    my $state = shift;
    my $debug = shift;
    logger($debug, "Entering Action_obj::constructor",1);

    my %actions;
    my $self = {
        _actions => \%actions,
        _dlpxObject => $dlpxObject,
        _startTime => $startTime,
        _endTime => $endTime,
        _state => $state,
        _debug => $debug
    };
    
    bless($self,$classname);
    my $detz = $dlpxObject->getTimezone();
    $self->{_timezone} = $detz;
    $self->loadActionList();
    return $self;
}


# Procedure getStartTimeWithTZ
# parameters: 
# - reference
# Return fault date with engine time zone

sub getStartTimeWithTZ {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getStartTimeWithTZ",1);    
    my $tz = new Date::Manip::TZ;
    my $action = $self->{_actions}->{$reference};
    my $ts = $action->{startTime};
    $ts =~ s/\....Z//;
    my $dt = ParseDate($ts);
    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $self->{_timezone});
    return sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
}


# Procedure waitForAction
# parameters: 
# - reference
# Return action details

sub waitForAction {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::waitForAction",1);    
    while( ($self->getState($reference) eq 'WAITING' ) ) {
        sleep 10;
        $self->loadActionList();
    }
}

# Procedure checkStateWithChild
# parameters: 
# - reference
# Return action details

sub checkStateWithChild {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::checkStateWithChild",1);    
    # wait for action to complete
    $self->waitForAction($reference);
    my $ret = $self->getState($reference);

    if (defined($self->{_parent_action}->{$reference})) {
        $ret = $self->checkStateWithChild($self->{_parent_action}->{$reference});
    } 

    # if Action failed - check if there was a job and print last message for job
    if ( $self->getState($reference) eq 'FAILED' ) {
        my $job = new Jobs_obj($self->{_dlpxObject}, undef, 'false', $self->{_debug}); 
        $job->getJobForAction($reference);
        print $job->getLastMessage() . "\n";
    }

    return $ret;
}

# Procedure getTitle
# parameters: 
# - reference
# Return action details

sub getTitle {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getTitle",1);    
    return $self->{_actions}->{$reference}->{title};
}


# Procedure getState
# parameters: 
# - reference
# Return action details

sub getState {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getState",1);    
    return $self->{_actions}->{$reference}->{state};
}

# Procedure getUserName
# parameters: 
# - reference
# Return action username

sub getUserName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getUserName",1); 
    my $user =  defined($self->{_actions}->{$reference}->{workSourceName}) ?  lc $self->{_actions}->{$reference}->{workSourceName} : 'internal';   
    return $user;
}

# Procedure getUserRef
# parameters: 
# - reference
# Return action username

sub getUserRef {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getUserRef",1); 
    my $user =  defined($self->{_actions}->{$reference}->{user}) ?  $self->{_actions}->{$reference}->{user} : 'N/A';   
    return $user;
}


# Procedure getDetails
# parameters: 
# - reference
# Return action details

sub getDetails {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getDetails",1);    
    return $self->{_actions}->{$reference}->{details};
}


# Procedure getActionType
# parameters: 
# - reference
# Return action type

sub getActionType {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Action_obj::getActionType",1);    
    return $self->{_actions}->{$reference}->{actionType};
}


# Procedure getActionList
# - sort order asc / desc
# Return list of all faults ordered by action id

sub getActionList {
    my $self = shift;
    my $order = shift;
    my $typefilter = shift;
    my $userfilter = shift;

    logger($self->{_debug}, "Entering Action_obj::getActionList",1);    

    my $actions = $self->{_actions};
    my @ret;
    my @action_list;



    if (defined($typefilter)) {

        @action_list = map { $actions->{$_}->{actionType} =~ /\Q$typefilter/ ? ( $_ ) : () } keys %{$actions} ;
    } else {
        @action_list = keys %{$actions};
    }


    if (defined($userfilter)) {
        my $user = lc $userfilter;
        @action_list = map { $self->getUser($_) =~ /\Q$user/ ? ( $_ ) : () } @action_list ;
    } 

    if ( (defined($order)) && (lc $order eq 'desc' ) ) {
        @ret = sort  { Toolkit_helpers::sort_by_number($b, $a) } ( @action_list );
    } else {
        @ret = sort { Toolkit_helpers::sort_by_number($a, $b) } ( @action_list );
    }

    return \@ret;
}


# Procedure loadActionList
# parameters: none
# Load a list of role objects from Delphix Engine

sub loadActionList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Action_obj::loadActionList",1);   

    my $operation = "resources/json/delphix/action?pageSize=10000";

    if ($self->{_startTime}) {
        $operation = $operation . "&fromDate=" . $self->{_startTime};
    }
    
    if ($self->{_endTime}) {
        $operation = $operation . "&toDate=" . $self->{_endTime};
    }

    if ($self->{_state}) {
        $operation = $operation . "&state=" . $self->{_state};
    }

    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $actions = $self->{_actions};

        my %parent_action;

        for my $actionitem (@res) {
            $actions->{$actionitem->{reference}} = $actionitem;
            if (defined($actionitem->{parentAction})) {
                $parent_action{$actionitem->{parentAction}} = $actionitem->{reference};
            }
        } 

        $self->{_parent_action} = \%parent_action;
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;