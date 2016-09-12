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
# Program Name : Fault_obj.pm
# Description  : Delphix Engine Fault object
# It's include the following classes:
# - Fault_obj - class which map a Delphix Engine fault API object
# Author       : Marcin Przepiorowski
# Created      : 20 Jul 2015 (v2.0.X)
#
#


package Faults_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Date::Manip;


# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $startTime = shift;
    my $endTime = shift;
    my $status = shift;
    my $severity = shift;
    my $debug = shift;
    logger($debug, "Entering Faults_obj::constructor",1);

    my %faults;
    my $self = {
        _faults => \%faults,
        _dlpxObject => $dlpxObject,
        _startTime => $startTime,
        _endTime => $endTime,
        _status => $status,
        _severity => $severity,
        _debug => $debug
    };
    
    bless($self,$classname);
    my $detz = $dlpxObject->getTimezone();
    $self->{_timezone} = $detz;
    $self->loadFaultList($debug);
    return $self;
}


# Procedure getFaultTimeWithTZ
# parameters: 
# - reference
# Return fault date with engine time zone

sub getTimeWithTZ {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Faults_obj::getFaultTimeWithTZ",1);    
    my $tz = new Date::Manip::TZ;
    my $fault = $self->{_faults}->{$reference};
    my $ts = $fault->{dateDiagnosed};
    $ts =~ s/\....Z//;
    my $dt = ParseDate($ts);
    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $self->{_timezone});
    return sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
}


# Procedure getTitle
# parameters: 
# - reference
# Return fault title

sub getTitle {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Faults_obj::getTitle",1);    
    return $self->{_faults}->{$reference}->{title};
}


# Procedure getTarget
# parameters: 
# - reference
# Return fault title

sub getTarget {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Faults_obj::getTarget",1);    
    return $self->{_faults}->{$reference}->{targetName};
}


# Procedure getStatus
# parameters: 
# - reference
# Return fault title

sub getStatus {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Faults_obj::getStatus",1);    
    return $self->{_faults}->{$reference}->{status};
}

# Procedure getSeverity
# parameters: 
# - reference
# Return fault title

sub getSeverity {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Faults_obj::getSeverity",1);    
    return $self->{_faults}->{$reference}->{severity};
}


# Procedure resolveFault
# parameters: 
# - reference
# - ignore - true / false
# Resolve fault and ignore it if requested
# Return 0 if OK 1 if failed

sub resolveFault {
    my $self = shift;
    my $reference = shift;
    my $ignore = shift;
    my $ret;

    logger($self->{_debug}, "Entering Faults_obj::resolveFault",1);    

    my $operation = "resources/json/delphix/fault/" . $reference . "/resolve";

    my %fault_data = (
        "type" => "FaultResolveParameters",
        "comments" => "auto close"
    );

    if (defined($ignore) && ( $ignore eq "1" ) ) {
        $fault_data{"ignore"} = JSON::true;
    };
    
    my $json_data = encode_json(\%fault_data);
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);


    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        $ret = 1;
    }

    $self->loadFaultList();
    return $ret;
}

# Procedure getFault
# parameters: 
# - reference
# Return fault hash for specific fault reference

sub getFault {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Faults_obj::getFault",1);    
    my $faults = $self->{_faults};
    return $faults->{$reference}
}


# Procedure getFaultsList
# - sort order asc / desc
# Return list of all faults ordered by fault id

sub getFaultsList {
    my $self = shift;
    my $order = shift;

    logger($self->{_debug}, "Entering Faults_obj::getFaultsList",1);    

    my $faults = $self->{_faults};
    my @ret;

    if ( (defined($order)) && (lc $order eq 'desc' ) ) {
        @ret = sort  { Toolkit_helpers::sort_by_number($b, $a) } ( keys %{$faults} );
    } else {
        @ret = sort { Toolkit_helpers::sort_by_number($a, $b) } ( keys %{$faults} );
    }

    return \@ret;;
}


# Procedure getRolesList
# parameters: none
# Load a list of role objects from Delphix Engine

sub loadFaultList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Faults_obj::loadFaultList",1);   

    my $operation = "resources/json/delphix/fault?pageSize=100";

    if ($self->{_startTime}) {
        $operation = $operation . "&fromDate=" . $self->{_startTime};
    }
    
    if ($self->{_endTime}) {
        $operation = $operation . "&toDate=" . $self->{_endTime};
    }

    if ($self->{_status}) {
        $operation = $operation . "&status=" . $self->{_status};
    }

    if ($self->{_severity}) {
        $operation = $operation . "&severity=" . $self->{_severity};
    }

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->getJSONResult($operation);

    if ($retcode) {
        print "Problem with loading faults. Please run with -debug 2 flag or try with different version of API\n";
        exit -1;
    }

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $faults = $self->{_faults};

        for my $faultitem (@res) {
            $faults->{$faultitem->{reference}} = $faultitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;