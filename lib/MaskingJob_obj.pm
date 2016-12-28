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
    my $self = {
        _maskingjob => \%maskingjob,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    $self->loadMaskingJobList();
    return $self;
}


# Procedure verifyMaskingJobForContainer
# parameters: 
# - reference
# - jobname
# Return job ref is job is assigned to source and can be used
# undef otherwise

sub verifyMaskingJobForContainer {
    my $self = shift;
    my $container = shift;
    my $name = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::verifyMaskingJobForContainer",1);    
    
    my $contjobs = $self->getMaskingJobForContainer($container);
    
    my @refarray = grep { lc $self->getName($_) eq lc $name } @{$contjobs};
    
    if (scalar(@refarray) gt 1) {
      print "Too many jobs with same name defined in source\n";
      return undef;
    }
    
    if (scalar(@refarray) lt 1) {
      print "Job with name $name not defined in source database\n";
      return undef;
    }  
    
    return $refarray[-1];

}

# Procedure getMaskingJobForContainer
# parameters: 
# - reference
# Return a masking job reference

sub getMaskingJobForContainer {
    my $self = shift;
    my $container = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getMaskingJobForContainer",1);    
    my $jobs = $self->{_maskingjob};
    
    my @retarray = grep { defined($jobs->{$_}->{associatedContainer}) && ($jobs->{$_}->{associatedContainer} eq $container) } keys %{$jobs};
    
    return \@retarray;
}


# Procedure getAssociatedContainer
# parameters: 
# - reference
# Return a masking job name

sub getAssociatedContainer {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getAssociatedContainer",1);    
    my $maskingjob = $self->{_maskingjob};
    
    my $ret;
    
    if (defined($reference)) {
      if (defined($maskingjob->{$reference}) && defined($maskingjob->{$reference}->{associatedContainer})) {
        $ret = $maskingjob->{$reference}->{associatedContainer};
      } else {
        $ret = 'N/A';
      }
    } else {
      $ret = 'N/A';
    }
    
    return $ret;
}

# Procedure setAssociatedContainer
# parameters: 
# - job reference
# - containter reference
# Assign job to container

sub setAssociatedContainer {
    my $self = shift;
    my $jobref = shift;
    my $contref = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::setAssociatedContainer",1);    
    my $maskingjob = $self->{_maskingjob};
    my $ret;
    my $operation = "resources/json/delphix/maskingjob/" . $jobref;
    
    my %masking_hash = (
      "type" => "MaskingJob",
      "associatedContainer" => $contref
    );
    
    my $json_data = to_json(\%masking_hash);
    logger($self->{_debug}, $json_data,2);
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    
    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
      $ret = 0;
    } else {
        $ret = 1;
        if (defined($result->{error})) {
            print "Problem with assigning job " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }
    
    return $ret;
}

# Procedure getName
# parameters: 
# - reference
# Return a masking job name

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getName",1);    
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

# Procedure getMaskingJobByName
# parameters: 
# - name
# Return a masking job reference for a name

sub getMaskingJobByName {
    my $self = shift;
    my $name = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getMaskingJobByName",1);    
    my $maskingjob = $self->{_maskingjob};
        
    my @refarray = grep { lc $self->getName($_) eq lc $name } keys %{$maskingjob};
    
    if (scalar(@refarray) gt 1) {
      print "Too many jobs with same name\n";
      return undef;
    }
    
    if (scalar(@refarray) lt 1) {
      print "Job with name $name not defined\n";
      return undef;
    }  
    
    return $refarray[-1];
    
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

# Procedure getMaskingJobs
# parameters: 
# Return array of masking jobs

sub getMaskingJobs {
    my $self = shift;
    
    logger($self->{_debug}, "Entering MaskingJob_obj::getMaskingJobs",1);    
    my $maskingjob = $self->{_maskingjob};
    
    my @retarray = sort (keys %{$maskingjob});
    return \@retarray;
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
      } 

    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}

1;