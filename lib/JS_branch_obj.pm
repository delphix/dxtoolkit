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
# Program Name : JS_branch_obj.pm
# Description  : Delphix Engine JS branch
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#


package JS_branch_obj;

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
    my $template = shift;
    my $debug = shift;
    logger($debug, "Entering JS_branch_obj::constructor",1);

    my %jsbranches;
    my $self = {
        _jsbranches => \%jsbranches,
        _dlpxObject => $dlpxObject,
        _template => $template,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadJSBranchList($debug);
    return $self;
}


# Procedure getJSBranchByName
# parameters: 
# - name 
# Return branch reference for particular name

sub getJSBranchByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering JS_branch_obj::getJSBranchByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $branchitem ( sort ( keys %{$self->{_jsbranches}} ) ) {

        if ( $self->getName($branchitem) eq $name) {
            $ret = $branchitem; 
        }
    }

    return $ret;
}

# Procedure getBranch
# parameters: 
# - reference
# Return branch hash for specific branch reference

sub getBranch {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_branch_obj::getBranch",1);    

    my $jsbranches = $self->{_jsbranches};
    return $jsbranches->{$reference};
}


# Procedure getJSTemplateList
# parameters: 
# Return JS template list

sub getJSBranchList {
    my $self = shift;
    
    logger($self->{_debug}, "Entering JS_branch_obj::getJSBranchList",1);    

    my @arrret = sort (keys %{$self->{_jsbranches}} );

    return \@arrret;
}


# Procedure getName
# parameters: 
# - reference
# Return JS branch name for specific branch reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_branch_obj::getName",1);   

    my $jsbranches = $self->{_jsbranches};
    return $jsbranches->{$reference}->{name};
}



# Procedure loadJSBranchList
# parameters: none
# Load a list of branch objects from Delphix Engine

sub loadJSBranchList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering JS_branch_obj::loadJSBranchList",1);   

    my $operation = "resources/json/delphix/jetstream/branch?";

    if (defined($self->{_template})) {
        $operation = $operation . "dataLayout=" . $self->{_template} ;
    }


    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jsbranches = $self->{_jsbranches};

        for my $branchitem (@res) {
            $jsbranches->{$branchitem->{reference}} = $branchitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;