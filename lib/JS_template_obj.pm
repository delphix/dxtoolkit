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
# Program Name : JS_template_obj.pm
# Description  : Delphix Engine JS template
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#
#


package JS_template_obj;

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
    logger($debug, "Entering JS_template_obj::constructor",1);

    my %jstemplates;
    my $self = {
        _jstemplates => \%jstemplates,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadJSTemplateList($debug);
    return $self;
}


# Procedure getJSTemplateByName
# parameters: 
# - name 
# Return template reference for particular name

sub getJSTemplateByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering JS_template_obj::getJSTemplateByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $templateitem ( sort ( keys %{$self->{_jstemplates}} ) ) {

        if ( $self->getName($templateitem) eq $name) {
            $ret = $templateitem; 
        }
    }

    return $ret;
}

# Procedure getJSTemplate
# parameters: 
# - reference
# Return template hash for specific template reference

sub getJSTemplate {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_template_obj::getTemplate",1);    

    my $jstemplates = $self->{_jstemplates};
    return $jstemplates->{$reference};
}


# Procedure getJSActiveBranch
# parameters: 
# - reference
# Return active branch for template for specific template reference

sub getJSActiveBranch {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_template_obj::getJSActiveBranch",1); 

    my $jstemplates = $self->{_jstemplates};
    return $jstemplates->{$reference}->{activeBranch};
}

# Procedure getJSFirstOperation
# parameters: 
# - reference
# Return firstoperation for template for specific template reference

sub getJSFirstOperation {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_template_obj::getJSFirstOperation",1); 

    my $jstemplates = $self->{_jstemplates};
    return $jstemplates->{$reference}->{firstOperation};
}


# Procedure getJSTemplateList
# parameters: 
# Return JS template list

sub getJSTemplateList {
    my $self = shift;
    
    logger($self->{_debug}, "Entering JS_template_obj::getJSTemplateList",1);    

    my @arrret = sort (keys %{$self->{_jstemplates}} );

    return \@arrret;
}


# Procedure getName
# parameters: 
# - reference
# Return JS template name for specific template reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_template_obj::getName",1);   

    my $jstemplates = $self->{_jstemplates};
    return $jstemplates->{$reference}->{name};
}


# Procedure getProperties
# parameters: 
# - reference
# Return JS template properties hash for specific template reference

sub getProperties {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering JS_template_obj::getProperties",1);   

    my $jstemplates = $self->{_jstemplates};
    return $jstemplates->{$reference}->{properties};
}


# Procedure loadJSTemplateList
# parameters: none
# Load a list of template objects from Delphix Engine

sub loadJSTemplateList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering JS_template_obj::loadJSTemplateList",1);   

    my $operation = "resources/json/delphix/jetstream/template";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jstemplates = $self->{_jstemplates};

        for my $templateitem (@res) {
            $jstemplates->{$templateitem->{reference}} = $templateitem;
        } 
    }
}

1;