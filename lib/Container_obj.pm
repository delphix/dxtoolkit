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
# Program Name : Bookmark_obj.pm
# Description  : Delphix Engine bookmark object
# It's include the following classes:
# - Environment_obj - class which map a Delphix Engine bookmark API object
# Author       : Marcin Przepiorowski
# Created      : 02 Jul 2015 (v2.0.0)
#


package Bookmark_obj;

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
    logger($debug, "Entering Bookmark_obj::constructor",1);
    
    my %bookmarks;
    my $self = {
        _bookmarks => \%bookmarks,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->getEnvironmentList($debug);
    return $self;
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

# Procedure getEnvironmentList
# parameters: none
# Load a list of bookmark objects from Delphix Engine

sub getBookmarkList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::getBookmarkList",1); 

    my $operation = "resources/json/delphix/jetstream/bookmark";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    my @res = @{$result->{result}};

    my $bookmarks = $self->{_bookmarks};


    for my $envitem (@res) {
        $environments->{$envitem->{reference}} = $envitem;
    } 
}


1;