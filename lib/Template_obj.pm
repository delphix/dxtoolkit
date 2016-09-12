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
# Program Name : Template_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - Template_obj - class which map a Delphix Engine template API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#



package Template_obj;

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
    logger($debug, "Entering Template_obj::constructor",1);

    my %templates;
    my $self = {
        _templates => \%templates,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadTemplateList($debug);
    return $self;
}


# Procedure getTemplateByName
# parameters: 
# - name 
# Return template reference for particular name

sub getTemplateByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Template_obj::getTemplateByName",1);    
    my $ret;

    #print Dumper $$config;

    for my $templateitem ( sort ( keys %{$self->{_templates}} ) ) {

        if ( $self->getName($templateitem) eq $name) {
            $ret = $templateitem; 
        }
    }

    return $ret;
}

# Procedure getTemplate
# parameters: 
# - reference
# Return template hash for specific template reference

sub getTemplate {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getTemplate",1);    

    my $templates = $self->{_templates};
    return $templates->{$reference};
}


# Procedure getTemplateList
# parameters: 
# Return template list

sub getTemplateList {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getTemplateList",1);    

    return keys %{$self->{_templates}};
}


# Procedure getName
# parameters: 
# - reference
# Return template name for specific template reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Template_obj::getName",1);   

    my $templates = $self->{_templates};
    return $templates->{$reference}->{name};
}

# Procedure exportTemplate
# parameters: 
# - reference
# - location - directory
# Return 0 if no errors

sub exportTemplate {
    my $self = shift;
    my $reference = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Template_obj::exportTemplate",1);   

    my $filename =  $location . "/" . $self->getName($reference) . ".template";

    my $templates = $self->{_templates};

    open (my $FD, '>', "$filename") or die ("Can't open file $filename : $!");

    print "Exporting template into file $filename \n";

    print $FD to_json($templates->{$reference}, {pretty => 1});

    close $FD;

    return 0;
}

# Procedure importTemplate
# parameters: 
# - location - file name
# Return 0 if no errors

sub importTemplate {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Template_obj::importTemplate",1);   

    my $filename =  $location;

    my $loadedTemplate;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedTemplate = $json->decode(<$FD>);
    
    close $FD;



    delete $loadedTemplate->{reference};
    delete $loadedTemplate->{namespace};

    $self->loadTemplateList();

    if (defined($self->getTemplateByName($loadedTemplate->{name}))) {
        print "Template " . $loadedTemplate->{name} . " from file $filename already exist.\n";
        return 0;
    }

    print "Importing template from file $filename.";

    my $json_data = to_json($loadedTemplate);

    my $operation = 'resources/json/delphix/database/template';

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Import completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure updateTemplate
# parameters: 
# - location - file name
# Return 0 if no errors

sub updateTemplate {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Template_obj::updateTemplate",1);   

    my $filename =  $location;

    my $loadedTemplate;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedTemplate = $json->decode(<$FD>);
    
    close $FD;

    delete $loadedTemplate->{reference};
    delete $loadedTemplate->{namespace};



    $self->loadTemplateList();

    if (! defined($self->getTemplateByName($loadedTemplate->{name}))) {
        print "Template " . $loadedTemplate->{name} . " from file $filename doesn't exist. Can't update.\n";
        return 1;
    } 

    my $reference = $self->getTemplateByName($loadedTemplate->{name});

    print "Updating template " . $loadedTemplate->{name} . " from file $filename.";

    my $json_data = to_json($loadedTemplate);

    my $operation = 'resources/json/delphix/database/template/' . $reference;

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Update completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure loadTemplateList
# parameters: none
# Load a list of template objects from Delphix Engine

sub loadTemplateList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Template_obj::loadTemplateList",1);   

    my $operation = "resources/json/delphix/database/template";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $templates = $self->{_templates};

        for my $templateitem (@res) {
            $templates->{$templateitem->{reference}} = $templateitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;