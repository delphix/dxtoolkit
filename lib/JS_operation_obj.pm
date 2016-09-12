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
# Copyright (c) 2016 by Delphix. All rights reserved.
#
# Program Name : JS_operation_obj.pm
# Description  : Delphix Engine JS template
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#
#


package JS_operation_obj;

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
    my $operation_ref = shift;
    my $debug = shift;
    logger($debug, "Entering JS_operation_obj::constructor",1);

    my %jsoperations;
    my $self = {
        _jsoperations => \%jsoperations,
        _operation_ref => $operation_ref,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadJSOperationList($debug);
    return $self;
}


# Procedure getJSOperation
# parameters: 
# - reference
# Return operation hash for specific template reference

sub getJSOperation {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_operation_obj::getJSOperation",1);    

    my $jsoperations = $self->{_jsoperations};
    return $jsoperations->{$reference};
}




# Procedure getJSTemplateList
# parameters: 
# Return JS operation list

sub getJSOperationList {
    my $self = shift;
    
    logger($self->{_debug}, "Entering JS_operation_obj::getJSOperationList",1);    

    my @arrret = sort (keys %{$self->{_jsoperations}} );

    return \@arrret;
}


# Procedure getName
# parameters: 
# - reference
# Return JS operation name for specific operation reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_operation_obj::getName",1);   

    my $jsoperations = $self->{_jsoperations};
    return $jsoperations->{$reference}->{name};
}


# Procedure getEndTime
# parameters: 
# - reference
# Return JS operation endtime for specific operation reference

sub getEndTime {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_operation_obj::getEndTime",1);   

    my $jsoperations = $self->{_jsoperations};
    return $jsoperations->{$reference}->{endTime};
}


# Procedure loadJSTemplateList
# parameters: none
# Load a list of template objects from Delphix Engine

sub loadJSOperationList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering JS_operation_obj::loadJSTemplateList",1);   

    my $operation = "resources/json/delphix/jetstream/operation";

    if (defined($self->{_operation_ref})) {
        $operation = $operation . "/" . $self->{_operation_ref};
    }


    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        if (defined($self->{_operation_ref})) {
            my $jsoperations = $self->{_jsoperations};
            $jsoperations->{$result->{result}->{reference}} = $result->{result};

        } else {
            my @res = @{$result->{result}};
            my $jsoperations = $self->{_jsoperations};

            for my $opitem (@res) {
                $jsoperations->{$opitem->{reference}} = $opitem;
            }
        }
    }
}

1;