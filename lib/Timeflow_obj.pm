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
# Program Name : Timeflow_obj.pm
# Description  : Delphix Engine Timeflow object
# It's include the following classes:
# - Timeflow_obj - class which map a Delphix Engine timeflow API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#


package Timeflow_obj;

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
    logger($debug, "Entering Timeflow_obj::constructor",1);
    

    my %timeflows;
    my $self = {
        _timeflows => \%timeflows,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->getTimeflowList($debug);
    return $self;
}

# Procedure getSnapshotTime
# parameters: 
# - reference
# Return time stamp (YYYY-MM-DD HH24:MI:SS) for Timeflow object reference

sub getSnapshotTime {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Timeflow_obj::getSnapshotTime",1);   

    my $ts = defined($self->{_timeflows}->{$reference}->{parentPoint}->{timestamp}) ? $self->{_timeflows}->{$reference}->{parentPoint}->{timestamp} : '';
    chomp($ts); 
    $ts =~ s/T/ /;
    $ts =~ s/\.000Z//;

    return defined($ts) ? $ts : '';
}


# Procedure getParentSnapshot
# parameters: 
# - reference
# Return refrence to parent snapshot

sub getParentSnapshot {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Timeflow_obj::getParentSnapshot",1);   

    my $snap = $self->{_timeflows}->{$reference}->{parentSnapshot};
    return defined($snap) ? $snap : '';
}


# Procedure getContainer
# parameters: 
# - reference
# Return cointainer for reference

sub getContainer {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Timeflow_obj::getContainer",1);   

    return $self->{_timeflows}->{$reference}->{container};
}


# Procedure getName
# parameters: 
# - reference
# Return name for reference

sub getName {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Timeflow_obj::getName",1);   

    return $self->{_timeflows}->{$reference}->{name};
}


# Procedure getCurrentTimeflowForContainer
# parameters: 
# - cointainer - limit flow to container if defined
# Return timeflow reference for particular cointainer

sub getCurrentTimeflowForContainer {
    my $self = shift;
    my $container = shift;
    my $ret;

    logger($self->{_debug}, "Entering Timeflow_obj::getCurrentTimeflowForContainer",1);   
    my $operation = "resources/json/delphix/database/" . $container;
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if ($result->{status} eq 'OK') {
        $ret = $result->{result}->{currentTimeflow};
    } 

    return $ret;
}


# Procedure getFixedTimeForTimeFlow
# parameters: 
# - timeflow - timeflow
# - timestamp - timestmp to check and fix 
# Return exact timestamp for timestamp with minutes only (snapshot without logsync)

sub getFixedTimeForTimeFlow{
    my $self = shift;
    my $tf = shift;
    my $timestamp = shift;
    my $ret;

    logger($self->{_debug}, "Entering Timeflow_obj::getFixedTimeForTimeFlow",1);   

    my $op = 'resources/json/delphix/timeflow/' . $tf . "/timeflowRanges";
    my %flowrange = (
        "type" => "TimeflowRangeParameters"
    );
    my $json_data = encode_json(\%flowrange);
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($op, $json_data);

    my @res = @{$result->{result}};

    my $match = 0;

    for my $tfritem (@res) {
        if ($tfritem->{provisionable}) {
            # cut to minutes
            my $de_start_timestamp = $tfritem->{startPoint}->{timestamp};
            my $de_end_timestamp = $tfritem->{endPoint}->{timestamp};
            $tfritem->{startPoint}->{timestamp} =~ s/\d\d\.\d\d\dZ$//;
            $tfritem->{endPoint}->{timestamp} =~ s/\d\d\.\d\d\dZ$//;
            if (defined($timestamp)) {
                #print Dumper $timestamp;

                $timestamp =~ s/\d\d\.\d\d\dZ$//;

                #print Dumper $timestamp;

                if  ( ($tfritem->{startPoint}->{timestamp} le $timestamp) && ($tfritem->{endPoint}->{timestamp} ge $timestamp) ) {
                    print "GIT\n";
                    $match = $match + 1;
                    # no log sync 
                    if ($de_start_timestamp eq $de_end_timestamp) {
                        $ret = $de_start_timestamp;
                    } 
                }
                print $tf . ' - ' . $tfritem->{startPoint}->{timestamp} . " - " . $tfritem->{endPoint}->{timestamp} . "\n";
            }
        }
    } 

    #print Dumper $result_fmt;

    if ($match gt 1) {
        print "More than one timestamp match pattern\n";
        return undef;
    }



    return $ret;
}

# Procedure getTimeflowList
# parameters: - none
# Load timeflow objects from Delphix Engine

sub getTimeflowList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Timeflow_obj::getTimeflowList",1);   

    my $operation = "resources/json/delphix/timeflow";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my @res = @{$result->{result}};
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my $timeflows = $self->{_timeflows};


        for my $tfitem (@res) {
            $timeflows->{$tfitem->{reference}} = $tfitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

}

1;