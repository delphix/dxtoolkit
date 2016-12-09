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
# Program Name : MaskingJob_obj.pm
# Description  : Delphix Engine MaskingJob object
# It's include the following classes:
# - MaskingJob_obj - class which map a Delphix Engine action API object
# Author       : Marcin Przepiorowski
# Created      : 18 Nov 2016 (v2.X.X)
#
#


package MaskingJob_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Date::Manip;
use lib '../lib';


# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $startTime = shift;
    my $endTime = shift;
    my $debug = shift;
    logger($debug, "Entering MaskingJob_obj::constructor",1);

    my %maskingjob;
    my %conttomask;
    my $self = {
        _conttomask => \%conttomask,
        _maskingjob => \%maskingjob,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    $self->loadMaskingJobList();
    return $self;
}

# Procedure getMaskingJobForContainer
# parameters: 
# - reference
# Return a masking job reference

sub getMaskingJobForContainer {
    my $self = shift;
    my $container = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getMaskingJobForContainer",1);    
    my $conttomask = $self->{_conttomask};
    
    my $ret;
    
    print Dumper $container;
        
    if (defined($conttomask->{$container})) {
      $ret = $conttomask->{$container};
    } 
    
    return $ret;
}

# Procedure getMaskingJobName
# parameters: 
# - reference
# Return a masking job name

sub getMaskingJobName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getMaskingJobName",1);    
    my $maskingjob = $self->{_maskingjob};
    
    my $ret;
    
    if (defined($reference)) {
      if (defined($maskingjob->{$reference})) {
        $ret = $maskingjob->{$reference}->{name};
      } else {
        $ret = 'N/A';
      }
    } else {
      $ret = 'N/A';
    }
    
    return $ret;
}


# Procedure getMaskingJob
# parameters: 
# - reference
# Return a masking job reference

sub getMaskingJob {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getMaskingJob",1);    
    my $maskingjob = $self->{_maskingjob};
    
    my $ret;
        
    if (defined($maskingjob->{$reference})) {
      $ret = $maskingjob->{$reference};
    } 
    
    return $ret;
}


# Procedure loadMaskingJobList
# parameters: none
# Load a list of masking jobs objects from Delphix Engine

sub loadMaskingJobList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering MaskingJob_obj::loadMaskingJobList",1);   
    my $pageSize = 5000;
    my $offset = 0;
    my $operation = "resources/json/delphix/maskingjob";


    my $maskingjob = $self->{_maskingjob};
    
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) { 

      my @res = @{$result->{result}};
      #my $jobs = $self->{_jobs};
      
      for my $maskjobitem (@res) {
          $maskingjob->{$maskjobitem->{reference}} = $maskjobitem;
          if (defined($maskjobitem->{associatedContainer})) {
            $self->{_conttomask}->{$maskjobitem->{associatedContainer}} = $maskjobitem->{reference};
          }
      } 

    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}

1;