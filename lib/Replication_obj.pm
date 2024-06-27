
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
use Time::Seconds;
use version;

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
  my $classname  = shift;
  my $dlpxObject = shift;
  my $debug      = shift;
  logger( $debug, "Entering Replication_obj::constructor", 1 );

  my %replication;
  my %replication_state;
  my %replication_points;

  my %new_spec = (
    "type"=> "ReplicationSpec"
  );

  my $self = {
    _replication_points => \%replication_points,
    _replication_state  => \%replication_state,
    _replication        => \%replication,
    _dlpxObject         => $dlpxObject,
    _debug              => $debug,
    _new                => \%new_spec
  };

  bless( $self, $classname );

  $self->loadReplicationList($debug);

  if ( version->parse($self->{_dlpxObject}->getApi()) >= version->parse(1.5.0) ) {
    $self->loadReplicationState();
    $self->loadReplicationPoint();
  }

  return $self;
}

# Procedure setGroups
# parameters:
# - groups
# Set object or load a groups from DE

sub setGroups {
  my $self   = shift;
  my $groups = shift;

  logger( $self->{_debug}, "Entering Replication_obj::setGroups", 1 );

  if ( !defined( $self->{_groups} ) ) {
    if ( defined($groups) ) {
      $self->{_groups} = $groups;
    }
    else {
      my $local_groups = new Group_obj( $self->{_dlpxObject}, $self->{_debug} );
      $self->{_groups} = $local_groups;
    }
  }
}

# Procedure setDatabases
# parameters:
# - databases
# Set object or load a databases from DE

sub setDatabases {
  my $self      = shift;
  my $databases = shift;

  logger( $self->{_debug}, "Entering Replication_obj::setDatabases", 1 );

  if ( !defined( $self->{_databases} ) ) {
    if ( defined($databases) ) {
      $self->{_databases} = $databases;
    }
    else {
      my $local_databases = new Databases( $self->{_dlpxObject}, $self->{_debug} );
      $self->{_databases} = $local_databases;
    }
  }
}

# Procedure getReplication
# parameters:
# - reference
# Return Replication hash for specific replication reference

sub getNamespace {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getNamespace", 1 );

  my $replication = $self->{_replication};
  return $replication->{$reference};
}


# Procedure isSDD
# parameters:
# - reference
# Return True if SDD

sub isSDD {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::isSDD", 1 );

  my $replication = $self->{_replication};
  if ($replication->{$reference}->{objectSpecification}->{type} eq 'ReplicationList') {
    return 0;
  } else {
    return 1;
  }
}




# Procedure getReplicationByName
# parameters:
# Return replication refernce for name

sub getReplicationByName {
  my $self = shift;
  my $name = shift;
  my $silent = shift;
  my $ret;
  logger( $self->{_debug}, "Entering Replication_obj::getReplicationByName", 1 );
  my @list = grep { $self->getName($_) eq $name } keys %{ $self->{_replication} };
  if ( scalar(@list) < 1 ) {
    if (!defined($silent)) {
      print "Can't find replication specification using name - $name\n";
    }
  }
  elsif ( scalar(@list) > 1 ) {
    print "Too many replication specification using same name - $name\n";
  }
  else {
    $ret = $list[-1];
  }
  return $ret;
}

# Procedure getReplicationByTag
# parameters:
# Return replication refernce for tag

sub getReplicationByTag {
  my $self = shift;
  my $tag  = shift;
  my $ret;
  logger( $self->{_debug}, "Entering Replication_obj::getReplicationByTag", 1 );
  my @list = grep { $self->getTag($_) eq $tag } keys %{ $self->{_replication} };
  if ( scalar(@list) < 1 ) {
    print "Can't find replication specification using tag - $tag\n";
  }
  elsif ( scalar(@list) > 1 ) {
    print "Too many replication specification using same tag - $tag\n";
  }
  else {
    $ret = $list[-1];
  }
  return $ret;
}

# Procedure getReplicationList
# parameters:
# Return replication list

sub getReplicationList {
  my $self = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getReplicationList", 1 );

  return sort ( keys %{ $self->{_replication} } );
}

# Procedure getName
# parameters:
# - reference
# Return replication name for specific replication reference

sub getName {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getName", 1 );

  my $replication = $self->{_replication};
  return $replication->{$reference}->{name};
}

# Procedure getTag
# parameters:
# - reference
# Return replication tag for specific replication reference

sub getTag {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getTag", 1 );

  my $replication = $self->{_replication};
  return $replication->{$reference}->{tag};
}

# Procedure getLastPoint
# parameters:
# - reference
# Return hash with last replication data (time, bytes, throughput)

sub getLastPoint {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getLastPoint", 1 );

  my $replication_state = $self->{_replication_state};

  my @stateforref = grep { $replication_state->{$_}->{spec} eq $reference } keys %{$replication_state};

  my %ret;

  if ( scalar(@stateforref) > 0 ) {
    my $last_replication_point_ref = $replication_state->{ $stateforref[-1] }->{lastPoint};

    my $last_point;

    if (defined($last_replication_point_ref)) {
      $last_point = $self->{_replication_points}->{$last_replication_point_ref};
    } else {
      $last_point = undef;
    }

    if ( defined( $last_point->{dataTimestamp} ) ) {
      my $timezone = $self->{_dlpxObject}->getTimezone();
      $ret{timestamp}  = Toolkit_helpers::convert_from_utc($last_point->{dataTimestamp}, $timezone, 1);
      $ret{throughput} = sprintf( "%9.2f", $last_point->{averageThroughput} / 1024 / 1024 );    #MB/s
      $ret{size}       = sprintf( "%9.2f", $last_point->{bytesTransferred} / 1024 / 1024 );     #MB
    }
    else {
      $ret{timestamp}  = 'N/A';
      $ret{throughput} = 'N/A';
      $ret{size}       = 'N/A';
    }

  }
  else {
    $ret{timestamp}  = 'N/A';
    $ret{throughput} = 'N/A';
    $ret{size}       = 'N/A';
  }

  return \%ret;
}

# Procedure getEnabled
# parameters:
# - reference
# Return replication status for specific replication reference

sub getEnabled {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getEnabled", 1 );

  my $replication = $self->{_replication};

  my $ret;

  #print Dumper $replication->{$reference};

  if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
    $ret = $replication->{$reference}->{enabled} ? 'ENABLED' : 'DISABLED';
  } else {
    $ret = $replication->{$reference}->{automaticReplication} ? 'ENABLED' : 'DISABLED';
  }

  return $ret;
}

# Procedure getTargetHost
# parameters:
# - reference
# Return replication status for specific replication reference

sub getTargetHost {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getTargetHost", 1 );

  my $replication = $self->{_replication};
  return $replication->{$reference}->{targetHost};
}


# Procedure getUser
# parameters:
# - reference
# Return user

sub getUser {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getUser", 1 );

  my $replication = $self->{_replication};
  return $replication->{$reference}->{targetPrincipal};
}

# Procedure getObjects
# parameters:
# - reference
# Return replication objects for specific replication reference

sub getObjects {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getObjects", 1 );

  my $replication = $self->{_replication};

  my $ret;

  if ( version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.5.0) ) {
    $ret = $replication->{$reference}->{objects};
  }
  else {
    if ( $replication->{$reference}->{objectSpecification}->{type} eq 'ReplicationList' ) {
      $ret = $replication->{$reference}->{objectSpecification}->{objects};
    }
    elsif ( $replication->{$reference}->{objectSpecification}->{type} eq 'ReplicationSecureList' ) {
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
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getObjectsName", 1 );

  my $replication = $self->{_replication};

  my @objnames;

  my $objects = $self->getObjects($reference);

  $self->setGroups();
  $self->setDatabases();

  if ( defined($objects) ) {

    for my $objitem ( sort ( @{$objects} ) ) {

      if ( $objitem =~ /DOMAIN/ ) {
        push( @objnames, 'DOMAIN' );
        last;
      }
      elsif ( $objitem =~ /GROUP/ ) {
        push( @objnames, $self->{_groups}->getName($objitem) );
      }
      else {
        my $db = $self->{_databases}->getDB($objitem);
        if ( defined($db) ) {
          my $name = $self->{_groups}->getName($db->getGroup()) . "/" . $db->getName();
          push( @objnames, $name );
        }
      }

    }

  }

  return join( ',', @objnames );

}

# Procedure getSchedule
# parameters:
# - reference
# Return replication objects for specific replication reference

sub getSchedule {
  my $self      = shift;
  my $reference = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getSchedule", 1 );

  my $replication = $self->{_replication};
  return $replication->{$reference}->{schedule};
}

# Procedure getLastJob
# parameters:
# - reference
# Return last replication job status for specific replication reference

sub getLastJob {
  my $self      = shift;
  my $reference = shift;
  my $zulu = shift;

  logger( $self->{_debug}, "Entering Replication_obj::getLastJob", 1 );

  my $jobs;

  $self->setGroups();
  $self->setDatabases();

  my $groups = $self->{_groups};

  my %job_data;
  my $job;

  if ( version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.5.0) ) {

    # find a job for particular DE based on title and message - this is for 4.1 only

    if ( !defined( $self->{_jobs} ) ) {

      #load jobs first
      $jobs =
        new Jobs( $self->{_dlpxObject},
        Toolkit_helpers::timestamp( $self->{_dlpxObject}->getTime( 1 * 24 * 60 ), $self->{_dlpxObject} ),
        undef, undef, undef, 'REPLICATION_SEND', undef, undef, undef, $self->{_debug} );
      $self->{_jobs} = $jobs;
    }
    else {
      $jobs = $self->{_jobs};
    }

    my $targetHost = $self->getTargetHost($reference);

    # take only one object
    my @refarray = grep { $_ =~ /DOMAIN|GROUP/ } @{ $self->getObjects($reference) };

    if ( scalar(@refarray) < 1 ) {

      #no groups or doamin - take any object
      @refarray = @{ $self->getObjects($reference) };
    }

    my $ref = $refarray[0];

    if ( $ref ne 'DOMAIN' ) {

      if ( $ref =~ /GROUP/ ) {

        # it is looking for a group name only here
        my $newname = $groups->getName($ref);

        if ( defined($newname) ) {
          $ref = $newname;
        }
      }
      else {
        my $dbgroup = $self->{_databases}->getDB($ref)->getGroup();
        my $newname = $groups->getName($dbgroup);
        if ( defined($newname) ) {
          $ref = $newname;
        }
      }
    }

    $job_data{'StartTime'} = 'N/A';
    $job_data{'State'}     = 'N/A';
    $job_data{'Runtime'}   = 'N/A';

    for my $jobitem ( @{ $jobs->getJobList('desc') } ) {

      $job = $jobs->getJob($jobitem);

      my $jobtitle = $job->getJobTitle();

      if ( $jobtitle =~ /\Q$targetHost/ ) {

        if ( $ref ne 'DOMAIN' ) {
          if ( $job->isFindMessage( 'Sending data for "' . $ref . '"' ) ) {
            $job_data{'StartTime'} = $job->getJobStartTimeWithTZ();
            $job_data{'State'}     = $job->getJobState();
            $job_data{'Runtime'}   = $job->getJobRuntime();
            last;
          }
        }
        else {
          $job_data{'StartTime'} = $job->getJobStartTimeWithTZ();
          $job_data{'State'}     = $job->getJobState();
          $job_data{'Runtime'}   = $job->getJobRuntime();
          last;
        }

      }

    }

    # end of 4.1 workaround
  }
  else {

    my @reparray = ($reference);

    $jobs =
      new Jobs( $self->{_dlpxObject},
      Toolkit_helpers::timestamp( $self->{_dlpxObject}->getTime( 1 * 24 * 60 ), $self->{_dlpxObject} ),
      undef, undef, undef, undef, undef, \@reparray, undef, 1, $self->{_debug} );

    $job_data{'StartTime'} = 'N/A';
    $job_data{'State'}     = 'N/A';
    $job_data{'Runtime'}   = 'N/A';

    for my $jobitem ( @{ $jobs->getJobList('desc') } ) {

      $job = $jobs->getJob($jobitem);

      my $jobtarget = $job->getJobTarget();

      if ( $jobtarget eq $reference ) {
        if (defined($zulu)) {
          $job_data{'StartTime'} = $job->getJobStartTime();
        } else {
          $job_data{'StartTime'} = $job->getJobStartTimeWithTZ();
        }
        $job_data{'State'}     = $job->getJobState();
        $job_data{'Runtime'}   = $job->getJobRuntime();
        last;
      }

    }
  }

  my $schedule = $self->getSchedule($reference);

  if ( defined($schedule) ) {
    if ( defined($job) ) {
      my $st_DT                = DateTime::Format::DateParse->parse_datetime( $job->getJobStartTimeWithTZ('offset') );
      my $replication_schedule = DateTime::Event::Cron::Quartz->new($schedule);
      my $next_replication     = $replication_schedule->get_next_valid_time_after($st_DT);
      $job_data{'NextRun'}  = $next_replication->strftime('%Y-%m-%d %T');
      $job_data{'Schedule'} = $schedule;
    }
    else {
      $job_data{'NextRun'}  = 'N/A';
      $job_data{'Schedule'} = $schedule;
    }
  }
  else {
    $job_data{'NextRun'}  = 'N/A';
    $job_data{'Schedule'} = 'N/A';
  }

  return \%job_data;
}

# Procedure loadReplicationPoint
# parameters: none
# Load a list of replication objects from Delphix Engine

sub loadReplicationPoint {
  my $self = shift;
  logger( $self->{_debug}, "Entering Replication_obj::loadReplicationPoint", 1 );

  my $operation = "resources/json/delphix/replication/serializationpoint";
  my ( $result, $result_fmt ) = $self->{_dlpxObject}->getJSONResult($operation);
  my $replication_points;
  if ( defined( $result->{status} ) && ( $result->{status} eq 'OK' ) ) {
    my @res = @{ $result->{result} };
    if ( scalar( @{ $result->{result} } ) ) {

      $replication_points = $self->{_replication_points};

      for my $repitem (@res) {
        $replication_points->{ $repitem->{reference} } = $repitem;
      }

    }
  }
  else {
    print "No data returned for $operation. Try to increase timeout \n";
  }

}

# Procedure loadReplicationState
# parameters: none
# Load a list of replication objects from Delphix Engine

sub loadReplicationState {
  my $self = shift;
  logger( $self->{_debug}, "Entering Replication_obj::loadReplicationState", 1 );

  my $operation = "resources/json/delphix/replication/sourcestate";
  my ( $result, $result_fmt ) = $self->{_dlpxObject}->getJSONResult($operation);
  my $replication_state;
  if ( defined( $result->{status} ) && ( $result->{status} eq 'OK' ) ) {
    my @res = @{ $result->{result} };
    if ( scalar( @{ $result->{result} } ) ) {

      $replication_state = $self->{_replication_state};

      for my $repitem (@res) {
        $replication_state->{ $repitem->{reference} } = $repitem;
      }

    }
  }
  else {
    print "No data returned for $operation. Try to increase timeout \n";
  }

}

# Procedure loadReplicationList
# parameters: none
# Load a list of replication objects from Delphix Engine

sub loadReplicationList {
  my $self = shift;
  logger( $self->{_debug}, "Entering Replication_obj::loadReplicationList", 1 );

  my $operation = "resources/json/delphix/replication/spec";
  my ( $result, $result_fmt ) = $self->{_dlpxObject}->getJSONResult($operation);
  if ( defined( $result->{status} ) && ( $result->{status} eq 'OK' ) ) {
    my @res = @{ $result->{result} };
    if ( scalar( @{ $result->{result} } ) ) {

      my $replication = $self->{_replication};

      for my $repitem (@res) {
        $replication->{ $repitem->{reference} } = $repitem;
      }

    }
  }
  else {
    print "No data returned for $operation. Try to increase timeout \n";
  }
}

# Procedure replicate
# parameters:
# - reference
# Kick off replication of particular profile using refrence
# Return job number if job started or undef otherwise

sub replicate {
  my $self      = shift;
  my $reference = shift;
  logger( $self->{_debug}, "Entering Replication_obj::replicate", 1 );
  my $operation = "resources/json/delphix/replication/spec/" . $reference . "/execute";
  return $self->runJobOperation( $operation, "{}" );
}

# Procedure runJobOperation
# parameters:
# - operation - API string
# - json_data - JSON encoded data
# Run POST command running background job for particular operation and json data
# Return job number if job started or undef otherwise

sub runJobOperation {
  my $self      = shift;
  my $operation = shift;
  my $json_data = shift;
  my $action    = shift;

  logger( $self->{_debug}, "Entering Replication_obj::runJobOperation", 1 );
  logger( $self->{_debug}, $operation, 2 );

  my ( $result, $result_fmt ) = $self->{_dlpxObject}->postJSONData( $operation, $json_data );
  my $jobno;

  if ( defined( $result->{status} ) && ( $result->{status} eq 'OK' ) ) {
    if ( defined($action) && $action eq 'ACTION' ) {
      $jobno = $result->{action};
    }
    else {
      $jobno = $result->{job};
    }
  }
  else {
    if ( defined( $result->{error} ) ) {
      print "Problem with starting job\n";
      print "Error: " . Toolkit_helpers::extractErrorFromHash( $result->{error}->{details} ) . "\n";
      logger( $self->{_debug}, "Can't submit job for operation $operation",   1 );
      logger( $self->{_debug}, "error " . Dumper $result->{error}->{details}, 1 );
      logger( $self->{_debug}, $result->{error}->{action},                    1 );
    }
    else {
      print "Unknown error. Try with debug flag\n";
    }
  }

  return $jobno;
}

# Procedure getReplicationObj
# parameters:
# - repref - current replication id to process
# Return a Replication Obj, new if repref is not defined

sub getReplicationObj {
  my $self = shift;
  my $repref = shift;

  if (defined($repref)) {
    if (defined($self->{_replication}->{$repref})) {
      delete $self->{_replication}->{$repref}->{reference};
      delete $self->{_replication}->{$repref}->{namespace};
      delete $self->{_replication}->{$repref}->{runtime};
      delete $self->{_replication}->{$repref}->{tag};
      delete $self->{_replication}->{$repref}->{objectSpecification}->{namespace};
      delete $self->{_replication}->{$repref}->{objectSpecification}->{name};
      delete $self->{_replication}->{$repref}->{objectSpecification}->{reference};

      return $self->{_replication}->{$repref};
    } else {
      logger( $self->{_debug}, "replication spec not found", 2 );
      print Dumper "replication spec not found";
      return undef;
    }
  } 
}


# Procedure backup
# parameters:
# - backup - path to backup directory
# - repref - current replication id to process
# Generate replication backup

sub backup {
  my $self = shift;
  my $path = shift;
  my $repref = shift;
  my $enginename = shift;

  
  logger( $self->{_debug}, "Entering Replication_obj::backup", 1 );
  logger( $self->{_debug}, "Obj rep " . $repref , 2 );

  my $backup_cmd = "dx_ctl_replication -d " . $enginename . " -action create -profilename " . $self->getName($repref);
  $backup_cmd = $backup_cmd . " -objects " . $self->getObjectsName($repref);
  $backup_cmd = $backup_cmd . " -host " . $self->getTargetHost($repref);
  $backup_cmd = $backup_cmd . " -user ". $self->getUser($repref);
  $backup_cmd = $backup_cmd . " -password xxxxxxxx";
  if (uc $self->getEnabled($repref) eq 'ENABLED') {
    $backup_cmd = $backup_cmd . " -enabled yes";
    if (defined(my $schedule = $self->getSchedule($repref))) {
      $backup_cmd = $backup_cmd . " -schedule \"" . $schedule . "\"";
    }
  } else {
    $backup_cmd = $backup_cmd . " -enabled no";
  }

  if ($self->isSDD($repref)) {
    $backup_cmd = $backup_cmd . " -type sdd";
  } else {
    $backup_cmd = $backup_cmd . " -type replica";
  }

  return $backup_cmd;

}

# Procedure createreplication
# parameters:
# - repobj - replication object
# - profilename - profile name
# Generate replication backup

sub createreplication {
  my $self = shift;
  my $repobj = shift;
  my $profilename = shift;
  logger( $self->{_debug}, "Entering Replication_obj::createreplication", 1 );
  logger( $self->{_debug}, "Obj rep " . $profilename , 2 );
  $repobj->{"name"} = $profilename;

  my $json = to_json($repobj);
  my $operation = "resources/json/delphix/replication/spec/";
  return $self->runJobOperation( $operation, $json, 'ACTION' );

}

# Procedure updateprofile
# parameters:
# - repboj - current object
# Update a profile

sub updateprofile {
  my $self = shift;
  my $repobj = shift;
  my $repref = shift;

  logger( $self->{_debug}, "Entering Replication_obj::updateprofile", 1 );
  logger( $self->{_debug}, "Obj ref " . $repref , 2 );
  my $operation = "resources/json/delphix/replication/spec/" . $repref;
  my $json = to_json($repobj);
  return $self->runJobOperation( $operation, $json, 'ACTION' );
}

# Procedure deletereplication
# parameters:
# - repref - current replication id to process
# Delete a profile

sub deletereplication {
  my $self = shift;
  my $repref = shift;
  logger( $self->{_debug}, "Entering Replication_obj::deletereplication", 1 );
  logger( $self->{_debug}, "Obj ref " . $repref , 2 );
  my $operation = "resources/json/delphix/replication/spec/" . $repref . "/delete";
  return $self->runJobOperation( $operation, '{}', 'ACTION' );
}


# Procedure failover
# parameters:
# - repref - current replication id to process
# Delete a profile

sub failover {
  my $self = shift;
  my $repref = shift;
  logger( $self->{_debug}, "Entering Replication_obj::failover", 1 );
  logger( $self->{_debug}, "Obj ref " . $repref , 2 );
  my $operation = "resources/json/delphix/replication/spec/" . $repref . "/delete";
  return $self->runJobOperation( $operation, '{}', 'ACTION' );
}

# Procedure setObjects
# parameters:
# - repobj - replication obj
# - object_names - array of names
# Set objects in objectSpecification

sub setObjects {
  my $self = shift;
  my $repobj = shift;
  my $object_names = shift;
  my $type = shift;

  $self->setGroups();
  $self->setDatabases();

  my @object_list = ();

  for my $objname ( sort ( @{$object_names} ) ) {

    if ((my $groupname, my $dbname) = $objname =~ '(.*)/(.*)') {

      my $grpobj = $self->{_groups}->getGroupByName($groupname);

      if (!defined($grpobj)) {
        print Dumper "Group not found";
        print Dumper $groupname;
        return undef;
      }
        
      my $dbobj = Toolkit_helpers::get_dblist_from_filter(undef, $groupname, undef, $dbname, $self->{_databases}, $self->{_groups});

      if (!defined($dbobj)) {
        print Dumper "database not found";
        return undef;
      }

      if (scalar(@{$dbobj}) ne 1 ) {
        print Dumper "database name is not unique";
        return undef;
      }

      push(@object_list, $dbobj->[0]);
    } else {
      
      if (uc $objname eq 'DOMAIN') {
        @object_list = ('DOMAIN');
        last;
      } else {
        my $grpobj = $self->{_groups}->getGroupByName($objname);
        if (!defined($grpobj)) {
          print Dumper "Group not found";
          print Dumper $objname;
          return undef;
        }
        push(@object_list, $grpobj->{reference});
      }

    }

  }


  if (lc $type eq 'replica') {
    $repobj->{"objectSpecification"}->{type} = 'ReplicationList';
    $repobj->{"objectSpecification"}->{"objects"} = \@object_list;
  } elsif (lc $type eq 'sdd') {
    $repobj->{"objectSpecification"}->{type} = 'ReplicationSecureList';
    $repobj->{"objectSpecification"}->{"containers "} = \@object_list;
  } else {
    print "Unknown type\n";
    return undef;
  }
  return 1;

}

# Procedure setHost
# parameters:
# - repobj - replication obj
# - host - host
# - username - username
# - password - password
# Set host related data

sub setHost {
  my $self = shift;
  my $repobj = shift;
  my $host = shift;
  my $user = shift;
  my $password = shift;

  if (defined($host)) {
    $repobj->{"targetHost"} = $host;
  }

  if (defined($user)) {
    $repobj->{"targetPrincipal"} = $user;
  }

  if (defined($password)) {
    $repobj->{"targetCredential"}->{"type"} = "PasswordCredential";
    $repobj->{"targetCredential"}->{"password"} = $password;
  }

}

# Procedure setEnabled
# parameters:
# - repobj
# Set enabled and set schedule

sub setEnabled {
  my $self      = shift;
  my $repobj    = shift;
  my $schedule  = shift;
  my $enabled   = shift;

  if ($enabled eq 'yes') {
    $enabled = JSON::true;
    $repobj->{"schedule"} = $schedule;
  } else {
    $enabled = JSON::false;
  }

  logger( $self->{_debug}, "Entering Replication_obj::setEnabled", 1 );

  my $replication = $self->{_replication};

  my $ret;

  if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.9.0)) {
    $repobj->{enabled} = $enabled;
  } else {
    $repobj->{automaticReplication} = $enabled;
  }

  
}



1;
