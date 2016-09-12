=begin
# Program Name : Replication_obj.pm
# Description  : Delphix Engine Replication object
# It's include the following classes:
# - Namespace_obj - class which map a Delphix Engine Replication API object
# Author       : Marcin Przepiorowski
# Created: 02 Sep 2015 (v2.0.0)
#
#
# Copyright (c) 2015 by Delphix. All rights reserved.
#
=cut

package Replication_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Jobs;
use Group_obj;
use Databases;
use DateTime::Event::Cron::Quartz;
use DateTime::Format::DateParse;

# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Replication_obj::constructor",1);

    my %replication;
    my $self = {
        _replication => \%replication,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);

    my $groups = new Group_obj($self->{_dlpxObject},$self->{_debug});
    $self->{_groups} = $groups;

    my $databases = new Databases($self->{_dlpxObject},$self->{_debug});
    $self->{_databases} = $databases;

    $self->loadReplicationList($debug);
    return $self;
}



# Procedure getReplication
# parameters: 
# - reference
# Return Replication hash for specific replication reference

sub getNamespace {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getNamespace",1);    

    my $replication = $self->{_replication};
    return $replication->{$reference};
}


# Procedure getReplicationList
# parameters: 
# Return replication list

sub getReplicationList {
    my $self = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getReplicationList",1);    

    return sort ( keys %{$self->{_replication}} );
}


# Procedure getName
# parameters: 
# - reference
# Return replication name for specific replication reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getName",1);   

    my $replication = $self->{_replication};
    return $replication->{$reference}->{name};
}


# Procedure getEnabled
# parameters: 
# - reference
# Return replication status for specific replication reference

sub getEnabled {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getEnabled",1);   

    my $replication = $self->{_replication};
    return $replication->{$reference}->{enabled} ? 'ENABLED' : 'DISABLED' ;
}


# Procedure getTargetHost
# parameters: 
# - reference
# Return replication status for specific replication reference

sub getTargetHost {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getTargetHost",1);   

    my $replication = $self->{_replication};
    return $replication->{$reference}->{targetHost};
}

# Procedure getObjects
# parameters: 
# - reference
# Return replication objects for specific replication reference

sub getObjects {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getObjects",1);   

    my $replication = $self->{_replication};

    my $ret;

    if ($self->{_dlpxObject}->getApi() lt '1.5') {
        $ret = $replication->{$reference}->{objects};
    } else {
        if ($replication->{$reference}->{objectSpecification}->{type} eq 'ReplicationList') {
           $ret = $replication->{$reference}->{objectSpecification}->{objects};
        } elsif ($replication->{$reference}->{objectSpecification}->{type} eq 'ReplicationSecureList') {
           $ret = $replication->{$reference}->{objectSpecification}->{containers};
        }
    }

    return $ret;
}


# Procedure getObjectsName
# parameters: 
# - reference
# Return replication objects for specific replication reference

sub getObjectsName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getObjectsName",1);   

    my $replication = $self->{_replication};

    my @objnames;
    
    my $objects = $self->getObjects($reference);
    
    if (defined($objects)) {

      for my $objitem  ( sort ( @{$objects} ) ) {

        if ($objitem =~ /DOMAIN/) {
            push (@objnames, 'DOMAIN');
            last;
        } elsif ($objitem =~ /GROUP/) {
            push (@objnames, $self->{_groups}->getName($objitem) );
        } else {
            my $db = $self->{_databases}->getDB($objitem);
            if (defined($db)) {
                push (@objnames, $db->getName() );
            }
        }

      }

    }

    return join(',', @objnames);

}

# Procedure getSchedule
# parameters: 
# - reference
# Return replication objects for specific replication reference

sub getSchedule {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getSchedule",1);   

    my $replication = $self->{_replication};
    return $replication->{$reference}->{schedule};
}

# Procedure getLastJob
# parameters: 
# - reference
# Return last replication job status for specific replication reference

sub getLastJob {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Replication_obj::getLastJob",1);   

    my $jobs;
    my $groups = $self->{_groups};

    if ( ! defined($self->{_jobs})) {
        #load jobs first
        $jobs = new Jobs($self->{_dlpxObject}, Toolkit_helpers::timestamp($self->{_dlpxObject}->getTime(1*24*60),$self->{_dlpxObject}), undef, undef, undef, 'REPLICATION_SEND', undef, $self->{_debug});
        $self->{_jobs} = $jobs;
    } else {
        $jobs = $self->{_jobs};
    } 


    my %job_data;
    my $job;


    if ($self->{_dlpxObject}->getApi() lt '1.5') {
        # find a job for particular DE based on title and message - this is for 4.1 only 

        my $targetHost = $self->getTargetHost($reference);

        # take only one object
        my @refarray = grep { $_ =~ /DOMAIN|GROUP/ } @{$self->getObjects($reference)};

        if (scalar(@refarray) < 1) {
            #no groups or doamin - take any object
            @refarray = @{$self->getObjects($reference)};
        }

        my $ref = $refarray[0];

        if ($ref ne 'DOMAIN') {

            if ($ref =~ /GROUP/ ) {
                # it is looking for a group name only here
                my $newname = $groups->getName($ref);

                if (defined($newname)) {
                    $ref = $newname;
                }
            } else {
                my $dbgroup = $self->{_databases}->getDB($ref)->getGroup();
                my $newname = $groups->getName($dbgroup);
                if (defined($newname)) {
                    $ref = $newname;
                }            
            }
        }

        $job_data{'StartTime'} = 'N/A';
        $job_data{'State'} = 'N/A';
        $job_data{'Runtime'} = 'N/A';
        
        for my $jobitem (@{$jobs->getJobList('desc')}) {

          $job = $jobs->getJob($jobitem);

          my $jobtitle = $job->getJobTitle();

          if ($jobtitle =~ /\Q$targetHost/ ) {
            
            if ($ref ne 'DOMAIN') {
                if ($job->isFindMessage('Sending data for "' . $ref . '"')) {
                    $job_data{'StartTime'} = $job->getJobStartTimeWithTZ();
                    $job_data{'State'} = $job->getJobState();
                    $job_data{'Runtime'} = $job->getJobRuntime();
                    last;
                }
            } else {
                $job_data{'StartTime'} = $job->getJobStartTimeWithTZ();
                $job_data{'State'} = $job->getJobState();
                $job_data{'Runtime'} = $job->getJobRuntime();
                last;
            }


          }

        }

    # end of 4.1 workaround
    } else {

        $job_data{'StartTime'} = 'N/A';
        $job_data{'State'} = 'N/A';
        $job_data{'Runtime'} = 'N/A';

        for my $jobitem (@{$jobs->getJobList('desc')}) {

          $job = $jobs->getJob($jobitem);

          my $jobtarget = $job->getJobTarget();

          if ( $jobtarget eq $reference) {        
            $job_data{'StartTime'} = $job->getJobStartTimeWithTZ();
            $job_data{'State'} = $job->getJobState();
            $job_data{'Runtime'} = $job->getJobRuntime();
            last;
          }

        }
    }

    my $schedule = $self->getSchedule($reference);

    if (defined($schedule)) {
        if (defined($job)) {
            my $st_DT = DateTime::Format::DateParse->parse_datetime( $job->getJobStartTimeWithTZ('offset') );
            my $replication_schedule = DateTime::Event::Cron::Quartz->new($schedule);
            my $next_replication = $replication_schedule->get_next_valid_time_after($st_DT);
            $job_data{'NextRun'} = $next_replication->strftime('%Y-%m-%d %T');
            $job_data{'Schedule'} = $schedule;
        } else {
            $job_data{'NextRun'} = 'N/A';
            $job_data{'Schedule'} = $schedule;           
        }
    } else {
        $job_data{'NextRun'} = 'N/A';
        $job_data{'Schedule'} = 'N/A';
    }

    return \%job_data;
}


# Procedure loadReplicationList
# parameters: none
# Load a list of replication objects from Delphix Engine

sub loadReplicationList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Replication_obj::loadReplicationList",1);   

    my $operation = "resources/json/delphix/replication/spec";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        if ( scalar(@{$result->{result}}) ) {

            my $replication = $self->{_replication};

            for my $repitem (@res) {
                $replication->{$repitem->{reference}} = $repitem;
            }

        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;