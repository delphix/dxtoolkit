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
# Program Name : Namespace_obj.pm
# Description  : Delphix Engine Namespace object
# It's include the following classes:
# - Namespace_obj - class which map a Delphix Engine namespace API object
# Author       : Marcin Przepiorowski
# Created      : 02 Sep 2015 (v2.0.0)
#


package Namespace_obj;

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
    logger($debug, "Entering Namespace_obj::constructor",1);

    my %namespace;
    my $self = {
        _namespace => \%namespace,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadNamespaceList($debug);
    return $self;
}


# Procedure getNamespaceByName
# parameters: 
# - name 
# Return namespace reference for particular name

sub getNamespaceByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Namespace_obj::getNamespaceByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $namespaceitem ( sort ( keys %{$self->{_namespace}} ) ) {

        if ( $self->getName($namespaceitem) eq $name) {
            $ret = $namespaceitem; 
        }
    }

    return $ret;
}

# Procedure getNamespace
# parameters: 
# - reference
# Return namespace hash for specific namespace reference

sub getNamespace {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Namespace_obj::getNamespace",1);    

    my $namespaces = $self->{_namespace};
    return $namespaces->{$reference};
}


# Procedure getNamespaceList
# parameters: 
# Return namespace list

sub getNamespaceList {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Namespace_obj::getNamespaceList",1);    

    return keys %{$self->{_namespace}};
}


# Procedure getName
# parameters: 
# - reference
# Return namespace name for specific namespace reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Namespace_obj::getName",1);   

    my $namespaces = $self->{_namespace};
    return $namespaces->{$reference}->{name};
}




# Procedure loadNamespaceList
# parameters: none
# Load a list of template objects from Delphix Engine

sub loadNamespaceList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Namespace_obj::loadNamespaceList",1);   

    my $operation = "resources/json/delphix/namespace";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $namespaces = $self->{_namespace};

        for my $namespaceitem (@res) {
            $namespaces->{$namespaceitem->{reference}} = $namespaceitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;