=begin
# Program Name : Jobs.pm
# Description  : Delphix Engine list of all Jobs
# Author       : Marcin Przepiorowski
# Created: 31 Aug 2015 (v2.0.0)
#
#
# Copyright (c) 2015 by Delphix. All rights reserved.
#
=cut

package Jobs;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Jobs_obj;

# constructor
# parameters 
# - dlpxObject - connection to DE

# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $fromdate = shift;
    my $todate = shift;
    my $state = shift;
    my $targetname = shift;
    my $type = shift;
    my $jobref = shift;
    my $targetref = shift;
    my $debug = shift;

    my %jobs;
    
    logger($debug, "Entering Jobs_obj::constructor",1);
    my $self = {
        _dlpxObject => $dlpxObject,
        _debug => $debug,
        _jobs => \%jobs,
        _todate => $todate,
        _fromdate => $fromdate,
        _targetName => $targetname,
        _state => $state,
        _type =>$type,
        _targetref =>$targetref
    };
    
    bless($self,$classname);
    
    my $detz = $dlpxObject->getTimezone();
    $self->{_timezone} = $detz;
    if (defined($jobref)) {
        $self->setJob($jobref);
    } else {
        $self->loadJobs();
    }
    return $self;
}


# procedure getJobList
# parameters
# - sort order
# return a list of jobs ordered by job-refnum

sub getJobList {
    my $self = shift;
    my $order = shift;

    logger($self->{_debug}, "Entering Jobs::getJobList",1);    
    
    my @job_list = keys %{$self->{_jobs}};

    my @ret;

    if ( (defined($order)) && (lc $order eq 'desc' ) ) {
        @ret = sort  { $b cmp $a } ( @job_list );
    } else {
        @ret = sort  { Toolkit_helpers::sort_by_number($a, $b) } @job_list;
    }

    return \@ret;
}


# procedure getJob
# parameters
# - reference
# return a jobs_obj object 

sub getJob {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Jobs::getJob",1);    

    return $self->{_jobs}->{$reference};
}

# procedure setJob
# parameters
# - reference
# load a particular job
# return undef if job doesn't exist

sub setJob {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Jobs::setJob",1);    

    my $job = new Jobs_obj($self->{_dlpxObject}, $reference, 'true', $self->{_debug});
    $job->setTimezone($self->{_timezone});
    $self->{_jobs}->{$reference} = $job;

    return $job->getJobActionType();
}

# Procedure loadJobs
# Load job status from Delphix Engine

sub loadJobs {
   my $self = shift;

   logger($self->{_debug}, "Entering Jobs::loadJobs",1);  
   
   logger($self->{_debug}, "List of objects for jobs " . Dumper $self->{_targetref}, 2 );
   
   if (defined($self->{_targetref})) {
      for my $targetitem (sort @{$self->{_targetref}}) {
         $self->loadJobs_worker($targetitem);
      }
   } else {
      $self->loadJobs_worker();
   }
    
}


# Procedure loadJobs_worker
# Load job status from Delphix Engine for target

sub loadJobs_worker 
{
    my $self = shift;
    my $targetref = shift;
    my $pageSize = 5000;

    logger($self->{_debug}, "Entering Jobs::loadJobs_worker",1);    

    my $offset = 0;

    my $operation = "resources/json/delphix/job?pageSize=$pageSize&pageOffset=$offset&";
    
    if (defined($targetref)) {
      $operation = $operation . "target=" . $targetref . "&";
   }

    if (defined($self->{_fromdate})) {
        $operation = $operation . "fromDate=" . $self->{_fromdate} . "&";
    }

    if (defined($self->{_todate})) {
        $operation = $operation . "toDate=" . $self->{_todate} . "&";
    }

    if (defined($self->{_state})) {
        $operation = $operation . "jobState=" . uc $self->{_state} . "&";
    }

    my $total = 1;
    
    while ($total > 0) {
      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
      if (defined($result->{status}) && ($result->{status} eq 'OK')) { 

        my @res = @{$result->{result}};
        my $jobs = $self->{_jobs};
        

        if (scalar(@res) < $pageSize) {
           $total = 0;
        }
        
        
        $offset = $offset + 1;
        $operation =~ s/pageOffset=(\d*)/pageOffset=$offset/;

        for my $jobitem (@res) {
            my $job = new Jobs_obj($self->{_dlpxObject}, undef, 'true', $self->{_debug});
            $job->setTimezone($self->{_timezone});
            $job->setJob($jobitem->{reference}, $jobitem);

            my $targetname = $self->{_targetName};
            my $type = $self->{_type};

            if (defined($targetname)) {
                if ( ! ( $job->getJobTargetName() =~ /\Q$targetname/ ) ) {
                    next;
                }
            } 

            if (defined($type)) {
                if ( ! ( $job->getJobActionType() =~ /\Q$type/ ) ) {
                    next;
                }
            } 

            $jobs->{$jobitem->{reference}} = $job;
        } 

      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }
      
   }

}


1;