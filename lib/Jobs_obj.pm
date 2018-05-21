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
# Program Name : Jobs_obj.pm
# Description  : Delphix Engine Jobs object
# It's include the following classes:
# - Jobs_obj - class which map a Delphix Engine jobs API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
#


package Jobs_obj;
BEGIN { $| = 1 }

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Date::Manip;

# constructor
# parameters
# - dlpxObject - connection to DE
# - job - job reference
# - silet - display or not progress
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $job = shift;
    my $silent = shift;
    my $debug = shift;


    logger($debug, "Entering Jobs_obj::constructor",1);
    my $self = {
        _dlpxObject => $dlpxObject,
        _job => $job,
        _silent => $silent,
        _debug => $debug
    };

    bless($self,$classname);

    if (defined($job)) {
        $self->loadJob();
    }
    return $self;
}

# Procedure loadJob
# parameters: - none
# Load job status from Delphix Engine

sub loadJob
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::loadJob",1);

    my $operation = "resources/json/delphix/job/" . $self->{_job};
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        $self->{_joboutput} = $result->{result};
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

}

# Procedure setJob
# parameters:
# - jobref
# - job hash
# Load job status from Delphix Engine

sub setJob
{
    my $self = shift;
    my $job = shift;
    my $jobhash = shift;
    logger($self->{_debug}, "Entering Jobs_obj::setJob",1);


    $self->{_job} = $job;
    $self->{_joboutput} = $jobhash
}

# Procedure setTimezone
# Set timezone for a job

sub setTimezone
{
    my $self = shift;
    my $timezone = shift;

    logger($self->{_debug}, "Entering Jobs_obj::setTimezone",1);
    $self->{_timezone} = $timezone;
}


# Procedure getJobForAction
# parameters:
# - reference - action reference
# Load job status from Delphix Engine

sub getJobForAction
{
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Jobs_obj::getJobForAction",1);

    my $operation = "resources/json/delphix/action/" . $reference . "/getJob";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    $self->{_joboutput} = $result->{result};
}


# Procedure getJobName
# parameters: - none
# Return a job name (reference) for particular object

sub getJobName
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getJobName",1);
    return $self->{_joboutput}->{reference};
}

# Procedure getJobState
# parameters: - none
# Return a job status for particular object

sub getJobState
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getJobState",1);
    return $self->{_joboutput}->{jobState};
}

# Procedure getJobActionType
# parameters: - none
# Return a job status for particular object

sub getJobActionType
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getJobActionType",1);
    return $self->{_joboutput}->{actionType};
}

# Procedure getJobTargetName
# parameters: - none
# Return a job status for particular object

sub getJobTargetName
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getJobTargetName",1);
    return defined ($self->{_joboutput}->{targetName}) ? $self->{_joboutput}->{targetName} : '';
}

# Procedure getJobTarget
# parameters: - none
# Return a target for a particular job

sub getJobTarget
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getJobTarget",1);
    return defined($self->{_joboutput}->{target}) ? $self->{_joboutput}->{target} : undef;
}

# Procedure getJobTargetName
# parameters: - none
# Return a job status for particular object

sub getUser
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getUser",1);
    return $self->{_joboutput}->{user};
}


# Procedure getJobTitle
# parameters: - none
# Return a job title for particular object

sub getJobTitle
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getJobTitle",1);
    return $self->{_joboutput}->{title};
}


# Procedure getJobStartTimeWithTZ
# parameters:
# - offset
# Return job start date with engine time zone

sub getJobStartTimeWithTZ {
    my $self = shift;
    my $retoffset = shift;

    logger($self->{_debug}, "Entering Jobs_obj::getJobStartTimeWithTZ",1);
    my $ts = $self->{_joboutput}->{startTime};
    my $ret;

    if (defined($retoffset)) {
        $ret = Toolkit_helpers::convert_from_utc($ts, $self->{_timezone}, 1, 1);
    } else {
        $ret = Toolkit_helpers::convert_from_utc($ts, $self->{_timezone}, 1);
    }
    return $ret;
}

# Procedure getJobStartTime
# parameters:
# - offset
# Return job start date in ZULU

sub getJobStartTime {
    my $self = shift;
    my $retoffset = shift;

    logger($self->{_debug}, "Entering Jobs_obj::getJobStartTime",1);
    return $self->{_joboutput}->{startTime};  
}


# Procedure getJobUpdateTimeWithTZ
# parameters:
# Return job end date with engine time zone

sub getJobUpdateTimeWithTZ {
    my $self = shift;

    logger($self->{_debug}, "Entering Jobs_obj::getJobStartTimeWithTZ",1);
    my $ts = $self->{_joboutput}->{updateTime};
    return Toolkit_helpers::convert_from_utc($ts, $self->{_timezone}, 1);

}

# Procedure getJobRuntime
# parameters:
# - reference
# Return fault date with engine time zone

sub getJobRuntime {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Jobs_obj::getJobRuntime",1);
    my $st = $self->{_joboutput}->{startTime};
    $st =~ s/\....Z//;
    my $et = $self->{_joboutput}->{updateTime};
    $et =~ s/\....Z//;

    return sprintf("%02d:%02d:%02d", split(':',Delta_Format(DateCalc(ParseDate($st), ParseDate($et)), 2, "%hv:%mv:%sv")));
}

# Procedure getPercentage
# parameters: - none
# Return a job progress for particular object

sub getPercentage
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getPercentage",1);
    my $pct = $self->{_joboutput}->{percentComplete};
    return $pct;
}

# Procedure getLastMessage
# parameters: - none
# Return a last message

sub getLastMessage
{
    my $self = shift;
    logger($self->{_debug}, "Entering Jobs_obj::getLastMessage",1);
    my $events =  $self->{_joboutput}->{events};
    my $last_event = @{$events}[-1];
    return $last_event->{messageDetails}
}

# Procedure isMessage
# parameters:
# - test to find
# Return true if message is found

sub isFindMessage
{
    my $self = shift;
    my $text = shift;
    logger($self->{_debug}, "Entering Jobs_obj::isFindMessage",1);


    if (! defined($self->{_joboutput}->{events})) {
        $self->loadJob();
    }

    my $events =  $self->{_joboutput}->{events};

    my $ret = 0;

    for my $event ( @{$events} ) {
        if ($event->{messageDetails} =~ /\Q$text/ ) {
            $ret = 1;
            last;
        }

    }


    return $ret;
}


# Procedure cancel
# parameters: none
# return 0 if OK

sub cancel
{
    my $self = shift;
    my $action = shift;
    logger($self->{_debug}, "Entering Jobs_obj::cancel",1);
    my $ret;
    if ( ($self->getJobState() eq 'RUNNING' ) || ($self->getJobState() eq 'SUSPENDED' ) ) {
        $ret = $self->runAction('cancel');
    } else {
        print "Only running or suspended jobs can be canceled\n";
        $ret = 1;
    }
    return $ret;
}

# Procedure suspend
# parameters: none
# return 0 if OK

sub suspend
{
    my $self = shift;
    my $action = shift;
    logger($self->{_debug}, "Entering Jobs_obj::suspend",1);
    my $ret;
    if ( ($self->getJobState() eq 'RUNNING' ) ) {
        $ret = $self->runAction('suspend');
    } else {
        print "Only running jobs can be suspended\n";
        $ret = 1;
    }
    return $ret;
}

# Procedure resume
# parameters: none
# return 0 if OK

sub resume
{
    my $self = shift;
    my $action = shift;
    logger($self->{_debug}, "Entering Jobs_obj::resume",1);
    my $ret;
    if ( ($self->getJobState() eq 'SUSPENDED' ) ) {
        $ret = $self->runAction('resume');
    } else {
        print "Only suspended jobs can be resumed\n";
        $ret = 1;
    }
    return $ret;
}

# Procedure runAction
# parameters:
# - action
# return 0 if OK

sub runAction
{
    my $self = shift;
    my $action = shift;
    logger($self->{_debug}, "Entering Jobs_obj::runAction",1);

    my $operation = "resources/json/delphix/job/" . $self->{_job} . "/" . $action;
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, '{}');

    my $ret = 1;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        $ret = 0;
    } else {
        if (defined($result->{error})) {
            print "Problem with job \n" . $result->{error}->{details} . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    $self->loadJob();

    return $ret;
}


# Procedure waitForJob
# parameters: - none
# Wait for job to be finished ( completed / failed or canceled) and display status if not silent
# return last status

sub waitForJob
{
    my $self = shift;
    my $oldpct = 0;
    logger($self->{_debug}, "Entering Jobs_obj::waitForJob",1);

    my $jobno = $self->{_joboutput}->{reference};

    $oldpct = $self->getPercentage();

    if ( (defined ($self->{_silent})) &&  ($self->{_silent} eq 'true' )) {
        print "$oldpct";
    }

    while (($self->getJobState() ne 'COMPLETED') && ($self->getJobState() ne 'CANCELED') && ($self->getJobState() ne 'FAILED')) {
        sleep 1;
        logger($self->{_debug}, "Waiting for job to completed. current progress - " . $self->getPercentage() ,2);
        $self->loadJob();
        if ( (defined ($self->{_silent})) &&  ($self->{_silent} eq 'true' )) {
            my $newpct = $self->getPercentage();
            if (defined($newpct)) {
                if ($oldpct ne $newpct ) {
                    print " - " . $newpct;
                    $oldpct = $newpct;
                }
            }
        }
    }

    if ( (defined ($self->{_silent})) &&  ($self->{_silent} eq 'true' )) {
        print "\n";
        print "Job $jobno finished with state: " .  $self->getJobState() . "\n";
        if ($self->getJobState() ne 'COMPLETED') {
            print "Last message is: " . $self->getLastMessage() . "\n";
        }
    }

    return $self->getJobState();

}

1;
