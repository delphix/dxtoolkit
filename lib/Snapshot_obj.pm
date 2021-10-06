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
# Copyright (c) 2015,2017 by Delphix. All rights reserved.
#
# Program Name : Snapshot_obj.pm
# Description  : Delphix Engine Snapshot object
# It's include the following classes:
# - Snapshot_obj - class which map a Delphix Engine snapshot API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#
#


package Snapshot_obj;

use warnings;
use strict;
use Data::Dumper;
use Date::Manip;
use JSON;
use Toolkit_helpers qw (logger);

# constructor
# parameters
# - dlpxObject - connection to DE
# - container - database reference
# - debug - debug flag (debug on if defined)


sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $container = shift;
    my $traverseTimeflows = shift;
    my $debug = shift;
    my $startDate = shift;
    my $endDate = shift;
    my $snapshotref = shift;
    logger($debug, "Entering Snapshot_obj::constructor",1);


    my %snapshots;
    my $self = {
        _snapshots => \%snapshots,
        _container => $container,
        _dlpxObject => $dlpxObject,
        _traverseTimeflows => $traverseTimeflows,
        _debug => $debug,
        _startDate => $startDate,
        _endDate => $endDate,
        _timezone => ''
    };

    bless($self,$classname);

    if (not defined($snapshotref)) {
      $self->getSnapshotList($debug);
    }

    return $self;
}

# Procedure getContainer
# parameters: refrerence
# Return cointainer for reference

sub getContainer {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getContainer",1);

    return $self->{_container};
}


# Procedure getSnapshotType
# parameters: refrerence
# Return type for reference

sub getSnapshotType {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotType",1);

    return $self->{_snapshots}->{$reference}->{type};
}

# Procedure getContainer
# parameters: refrerence
# Return cointainer for reference

sub getFirstPoint {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getFirstPoint",1);

    return defined($self->{_snapshots}->{$reference}->{firstChangePoint}->{timestamp}) ? $self->{_snapshots}->{$reference}->{firstChangePoint}->{timestamp} : 'N/A';
}

# # Procedure getContainer
# # parameters: refrerence
# # Return cointainer for reference
#
# sub getLatestPoint {
#     my $self = shift;
#     my $reference = shift;
#     logger($self->{_debug}, "Entering Snapshot_obj::getFirstPoint",1);
#
#     return defined($self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp}) ? $self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp} : 'N/A';
# }


# Procedure getlatestChangePoint
# parameters: refrerence
# Return latest scn for snapshot - to detect if provision by scn was used

sub getlatestChangePoint {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getlatestChangePoint",1);

    return defined($self->{_snapshots}->{$reference}->{latestChangePoint}->{location}) ? $self->{_snapshots}->{$reference}->{latestChangePoint}->{location} : 'N/A';
}


# Procedure isProvisiable
# parameters: refrerence
# Return provisiable flag

sub isProvisionable {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::isProvisionable",1);

    return $self->{_snapshots}->{$reference}->{runtime}->{provisionable};
}

# Procedure getSnapshots
# parameters:
# - snapshot name
# Return list of snapshot reference

sub getSnapshots {
    my $self = shift;
    my $snapshotname = shift;
    my $ret;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshots",1);
    if (defined($snapshotname)) {
        my @retarr = grep { $self->getSnapshotName($_) eq $snapshotname } @{$self->{_snapshot_list}} ;
        $ret = \@retarr;
    } else {
        $ret = $self->{_snapshot_list} ;
    }
    return $ret;
}



# Procedure getSnapshotName
# parameters:
# Return list of snapshot reference

sub getSnapshotName {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotName",1);
    my $ret;
    if (defined($self->{_snapshots}->{$reference}->{name})) {
      $ret = $self->{_snapshots}->{$reference}->{name};
    } else {
      $ret = 'No snapshot data'
    }
    return $ret;
}


# Procedure getSnapshotCreationTime
# parameters:
# refrence
# Return snapshot creation time

sub getSnapshotCreationTime {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotCreationTime",1);
    return $self->{_snapshots}->{$reference}->{creationTime};
}

# Procedure getSnapshotCreationTimeWithTimezone
# parameters:
# refrence
# Return snapshot creation time with timezone

sub getSnapshotCreationTimeWithTimezone {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotCreationTimeWithTimezone",1);

    my $timezone = $self->getSnapshotTimeZone($reference);
    my $zulutime = $self->getSnapshotCreationTime($reference);
    my $ret;

    if ($timezone eq 'N/A') {
        $ret = 'N/A - timezone unknown';
    } else {

        my $creationDate = Toolkit_helpers::convert_from_utc($zulutime, $timezone, 1);

        if (defined($creationDate)) {
            $ret = $creationDate;
        } else {
            $ret = 'N/A';
        }
    }

    return $ret;
}


# Procedure getSnapshotByName
# parameters:
# - name
# Return snapshot reference for name

sub getSnapshotByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotByName",1);
    my @ret;

    @ret = grep { $self->getSnapshotName($_) eq $name } @{$self->{_snapshot_list}};

    return \@ret;
}


# Procedure getSnapshotVersion
# parameters:
# Return list of snapshot reference

sub getSnapshotVersion {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotVersion",1);
    return defined($self->{_snapshots}->{$reference}->{version}) ? $self->{_snapshots}->{$reference}->{version} : 'N/A' ;
}

# Procedure getSnapshotVersion
# parameters:
# Return list of snapshot reference

sub getSnapshotRetention {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotRetention",1);
    my $ret;
    if ($self->{_snapshots}->{$reference}->{retention} lt 0 ) {
        $ret = 'forever';
    } elsif ($self->{_snapshots}->{$reference}->{retention} eq 0 ) {
        $ret = 'Policy';
    } else {
        $ret = $self->{_snapshots}->{$reference}->{retention};
    }
    return $ret;
}


# Procedure getSnapshotTimeflow
# parameters:
# Return list of snapshot reference

sub getSnapshotTimeflow {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotTimeflow",1);
    return $self->{_snapshots}->{$reference}->{timeflow};
}

# Procedure getSnapshots
# parameters:
# Return list of snapshot reference

sub getSnapshotsByTimeflow {
    my $self = shift;
    my $timeflow = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotsByTimeflow",1);
    my @snaplist;

    for my $snapitem (@{$self->{_snapshot_list}}) {
        if ($self->getSnapshotTimeflow($snapitem) eq $timeflow) {
            push(@snaplist, $snapitem);
        }

    }

    return \@snaplist;
}

# Procedure getSnapshotTime
# parameters:
# - reference
# Return timestamp for snapshot

sub getSnapshotTime {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotTime",1);
    my $ts;

    if (defined($self->{_snapshots}->{$reference})) {
      $ts = $self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp};
    } else {
      # non existing snapshot - JS case
      return 'N/A';
    }

    # if $ts is null - I need to reconsider which one to use - latest change of snapshot process seems OK

    chomp($ts);
    $ts =~ s/T/ /;
    $ts =~ s/\....Z//;
    return $ts;
}

# Procedure checkTZ
# if timezone defined as GMT+/-offset return undef
# parameters:
# - timezone
# Return timezone of OK or N/A if timezone is not recognized by perl

sub checkTZ {
    my $self = shift;
    my $timezone = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::checkTZ",1);

    # fixes for timezones supported by Delphix but not recognized by Perl
    if ($timezone eq 'Etc/Zulu') {
        $timezone = 'UTC';
    }
    if ($timezone eq 'Zulu') {
        $timezone = 'UTC';
    }
    if ($timezone eq 'Etc/Universal') {
        $timezone = 'UTC';
    }
    if ($timezone eq 'Universal') {
        $timezone = 'UTC';
    }
    if ($timezone eq 'Etc/Greenwich') {
        $timezone = 'GMT';
    }
    if ($timezone eq 'Greenwich') {
        $timezone = 'GMT';
    }
    if ($timezone eq 'GMT0') {
        $timezone = 'GMT';
    }
    if ($timezone eq 'Etc/GMT0') {
        $timezone = 'GMT';
    }
    if ($timezone eq 'Etc/GMT-0') {
        $timezone = 'GMT';
    }
    if ($timezone eq 'Etc/GMT+0') {
        $timezone = 'GMT';
    }

    my $checktime = time();
    my $dt = ParseDate($checktime);
    my $tz = new Date::Manip::TZ;
    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $timezone);

    if (!$err) {
        logger($self->{_debug}, "checkTZ abbrev-" . $abbrev ,1);
    } else {
        logger($self->{_debug}, "checkTZ abbrev-undefined" ,1);
        $timezone = 'N/A';
    }

    return $timezone;
}

# Procedure getSnapshotTimeZone
# parameters:
# - reference
# Return timestamp for snapshot

sub getSnapshotTimeZone {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotTimeZone",1);

    if (!defined($self->{_snapshots}->{$reference})) {
      return 'N/A';
    }

    my $ts = $self->{_snapshots}->{$reference}->{timezone} ;

    if (!defined($self->{_snapshots}->{$reference}->{timezone})) {
      return 'N/A';
    }

    #print Dumper $self->{_snapshots}->{$reference};
    logger($self->{_debug}, "Snapshot timezone returned by DE $ts",1);
    chomp($ts);
    my @temp = split(',',$ts);
    my $ret = $temp[0];

    if (! ($ret =~ /[a-zA-Z]{3}.\d\d:\d\d/ )) {
      # if timezone is not GMT+-00:00 format
      # check if this format can be recognized and amended by checkTZ function
      $ret = $self->checkTZ($ret);
    }

    return $ret;
}

# Procedure getSnapshotTimewithzone
# parameters:
# - reference
# Return timestamp for snapshot with abv timezone and timezone

sub getSnapshotTimewithzone {
    my $self = shift;
    my $reference = shift;
    my $ret;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotTimewithzone",1);
    my $tz = new Date::Manip::TZ;
    my $zulutime = $self->getSnapshotTime($reference) ;

    if ($zulutime eq 'N/A') {
      return ('N/A','N/A');
    }

    my $timezone = $self->getSnapshotTimeZone($reference);

    if ($timezone eq 'N/A') {
        $ret = 'N/A - timezone unknown';
    } else {
        $ret = Toolkit_helpers::convert_from_utc($zulutime, $timezone, 1);
    }
    return ($ret,$timezone);
}

# Procedure getLatestSnapshotTime
# parameters:
# Return time of last snapshot with abv timezone and timezone

sub getLatestSnapshotTime {
    my $self = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getLatestSnapshotTime",1);
    my $reference = $self->{_snapshot_list}[-1];
    my $ret;
    my $timezone;
    if (defined($reference)) {
        ($ret,$timezone) = $self->getSnapshotTimewithzone($reference);
    } else {
        $ret = 'N/A';
    }
    return ($ret,$timezone);
}

# Procedure findTimeflowforTimestamp
# parameters:
# - timestamp
# Return timeflow for timestamp

sub findTimeflowforTimestamp {
    my $self = shift;
    my $timestamp = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::findTimeflowforTimestamp",1);

    my %ret;

    my $match = 0;
    for my $snapitem ( @{$self->getSnapshots()} ) {

        my $sttz = Toolkit_helpers::convert_to_utc($timestamp, $self->getSnapshotTimeZone($snapitem), undef, undef);

        my $snap_startpoint = $self->getStartPoint($snapitem);
        my $snap_endpoint = $self->getEndPoint($snapitem);
        my $full_snap_startpoint = $snap_startpoint;

        $snap_startpoint =~ s/T/ /;
        $snap_startpoint =~ s/\....Z//;
        $snap_endpoint =~ s/T/ /;
        $snap_endpoint =~ s/\....Z//;

        # change from "ge" $sttz to "gt" for snapshots
        # as end snapshot can = start snapshot and we should use
        # newer one
        # "ge" was for same start and end snapshot and now
        # a new if is added

        if ($snap_startpoint eq $snap_endpoint) {
          if  ( $snap_startpoint eq $sttz ) {
              $match = $match + 1;
              $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
              $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
              $ret{full_startpoint} = $full_snap_startpoint;
          }
        } else {

          if  ( ($snap_startpoint le $sttz) && ($snap_endpoint gt $sttz ) ) {
              $match = $match + 1;
              $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
              $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
              $ret{full_startpoint} = $full_snap_startpoint;
          }
        }
        if ($match gt 1) {
            print "Timestamp in more than one snapshot. Exiting\n";
            return undef;
        }
    }

    return \%ret;
}


# Procedure findTimeflowforLocation
# parameters:
# - timestamp
# Return timeflow for timestamp

sub findTimeflowforLocation {
    my $self = shift;
    my $location = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::findTimeflowforLocation",1);

    my %ret;


    my $match = 0;
    for my $snapitem ( @{$self->getSnapshots()} ) {


        my $snap_startpoint = $self->getStartPointLocation($snapitem);
        my $snap_endpoint = $self->getEndPointLocation($snapitem);

        if  ( ($snap_startpoint le $location) && ($snap_endpoint ge $location ) ) {
            $match = $match + 1;
            $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
            $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
        }
        if ($match gt 1) {
            print "Location in more than one snapshot. Exiting\n";
            return undef;
        }
    }

    return \%ret;
}

# Procedure findSnapshotforTimestamp
# parameters:
# - timestamp
# - timeflow
# - utc_timestamp
# if timestamp is without minutes only one snapshot per minute is allowed,
# if there is more then one error will be displayed
# timeflow will limit search for particular timeflow only
# utc_timestamp will skip conversion of timestamp
# Return snapshot for timestamp

sub findSnapshotforTimestamp {
    my $self = shift;
    my $timestamp = shift;
    my $timeflow = shift;
    my $utc_timestamp = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::findSnapshotforTimestamp",1);

    my %ret;
    #my $tz = new Date::Manip::TZ;
    #my $dt = ParseDate($timestamp);

    my $seconds;

    if ($timestamp =~ /^(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d\d)$/) {
      $seconds = 0;
    } else {
      $seconds = 1;
    }


    my $match = 0;

    my $snaplist;

    if (!defined($timeflow)) {
      $snaplist = $self->getSnapshots();
    } else {
      my @timeflowarray = grep { $self->{_snapshots}->{$_}->{timeflow} eq $timeflow } sort (@{$self->getSnapshots()});
      $snaplist = \@timeflowarray;
    }

    my $sttz;
    my $snap_startpoint;
    my $final_ts;
    my $snap_endpoint;
    my $snapitem;

    for $snapitem ( @{$snaplist} ) {

        #my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $self->getSnapshotTimeZone($snapitem));

        #my $sttz = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4]);

        if (defined($utc_timestamp)) {
          $sttz = Toolkit_helpers::convert_to_utc($timestamp, 'UTC', undef, undef);
        } else {
          $sttz = Toolkit_helpers::convert_to_utc($timestamp, $self->getSnapshotTimeZone($snapitem), undef, undef);
        }

        if ($seconds == 0) {
          # delete seconds from converted timestamp as input was given without seconds
          $sttz =~ s/\:\d\d$//;
        }

        $snap_startpoint = $self->getStartPoint($snapitem);
        $final_ts = $snap_startpoint;
        $snap_endpoint = $self->getEndPoint($snapitem);

        $snap_startpoint =~ s/T/ /;
        $snap_endpoint =~ s/T/ /;

        if ($seconds == 0) {
          $snap_startpoint =~ s/\:\d\d\.\d\d\dZ$//;
          $snap_endpoint =~ s/\:\d\d\.\d\d\dZ$//;
        } else {
          $snap_startpoint =~ s/.\d\d\dZ$//;
          $snap_endpoint =~ s/.\d\d\dZ$//;
        }


        logger($self->{_debug}, "entry ts " . $sttz . " tf " . Dumper $timeflow,2);
        logger($self->{_debug}, "startsnap " . $snap_startpoint,2);
        logger($self->{_debug}, "endsnap " . $snap_endpoint,2);

        # temporary ($snap_endpoint gt $sttz ) will be changed to ge
        # it will require more tests -

        # if not found run again with ge ?????

        if  ( ( ($snap_startpoint le $sttz) && ($snap_endpoint gt $sttz ) ) || ( ( $snap_startpoint eq $snap_endpoint ) && ($sttz eq $snap_startpoint) ) ) {
            $match = $match + 1;
            $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
            $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
            $ret{timestamp} = $final_ts;
            $ret{snapshotref} = $snapitem ;
            logger($self->{_debug}, "hit for snapshot " . $snapitem,2);
        }

    }


    if ($match eq 0) {
      logger($self->{_debug}, "checking for last snapshot with equal condition", 2);

      $snapitem = @{$snaplist}[-1];
      logger($self->{_debug}, "snapshot " . $snapitem , 2);

      # if match is 0 for last snapshot run check again but end time is ge to requested timestamp
      if  ( ( ($snap_startpoint le $sttz) && ($snap_endpoint ge $sttz ) ) || ( ( $snap_startpoint eq $snap_endpoint ) && ($sttz eq $snap_startpoint) ) ) {
          $match = $match + 1;
          $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
          $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
          $ret{timestamp} = $final_ts;
          $ret{snapshotref} = $snapitem ;
          logger($self->{_debug}, "hit for snapshot " . $snapitem,2);
      }
    }


    if ($match gt 1) {
        print "Timestamp in more than one snapshot. Add seconds to timestamp. Exiting\n";
        return undef;
    }

    return \%ret;
}

# Procedure getLatestTime
# parameters:
# Return time of last snapshot

sub getLatestTime {
    my $self = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getLatestTime",1);
    my $reference = $self->{_snapshot_list}[-1];
    return $self->getStartPoint($reference);
}


# Procedure getLastProvisionableSnapshot
# parameters:
# Return ref of last provisionable snapshot

sub getLastProvisionableSnapshot {
    my $self = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getLastProvisionableSnapshot",1);

    my $ret;

    my @reversed_snapshots = reverse @{$self->{_snapshot_list}};

    for my $s ( @reversed_snapshots ) {
      if ($self->isProvisionable($s)) {
        print "First provisionable snapshot found - " . $self->getSnapshotName($s) . "\n";
        $ret = $s;
        last;
      }
    }

    return $ret;
}


# Procedure getEndPointwithzone
# parameters:
# - reference
# Return time of last time in snapshot with timezone

sub getEndPointwithzone {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getEndPointwithzone",1);
    my $tz = new Date::Manip::TZ;
    my $zulutime = $self->getEndPoint($reference) ;
    if (defined($zulutime)) {
      my $timezone = $self->getSnapshotTimeZone($reference);
      return Toolkit_helpers::convert_from_utc($zulutime,$timezone,1);
    } else {
      return 'Error getting time';
    }
}

# Procedure getEndPoint
# parameters:
# Return time of last time in snapshot

sub getEndPoint {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getEndPoint",1);

    my $res;

    if (defined($self->{_snapshots}->{$reference}->{timeflowRange}) ) {
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    } else {
        if ($self->getTimeflowRange($reference) eq 'N/A') {
          return undef;
        }
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    }

    if ($res eq 'N/A') {
      return undef;
    }

    my $ts = defined($res->{endPoint}) ? $res->{endPoint}->{timestamp} : undef;
    return $ts;
}

# Procedure getStartPointwithzone
# parameters:
# - reference
# Return time of first point in snapshot in snapshot with timezone

sub getStartPointwithzone {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getStartPointwithzone",1);
    my $tz = new Date::Manip::TZ;
    my $zulutime = $self->getStartPoint($reference) ;

    if (defined($zulutime)) {
      my $timezone = $self->getSnapshotTimeZone($reference);
      return Toolkit_helpers::convert_from_utc($zulutime,$timezone,1);
    } else {
      return 'Error getting time';
    }
}

# Procedure getStartPoint
# parameters:
# Return time of first point in snapshot

sub getStartPoint {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getStartPoint",1);

    my $res;

    if (defined($self->{_snapshots}->{$reference}->{timeflowRange}) ) {
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    } else {
        if ($self->getTimeflowRange($reference) eq 'N/A') {
          return undef;
        }
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    }
    my $ts = defined($res->{startPoint}) ? $res->{startPoint}->{timestamp} : undef;
    return $ts;
}

# Procedure getStartPointLocation
# parameters:
# Return location of first point in snapshot

sub getStartPointLocation {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getStartPointLocation",1);

    my $res;
    if (defined($self->{_snapshots}->{$reference}->{timeflowRange}) ) {
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    } else {
        $self->getTimeflowRange($reference);
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    }
    my $ts = defined($res->{startPoint}) ? $res->{startPoint}->{location} : undef;
    return defined($ts) ? $ts : 'N/A';
}


# Procedure getEndPointLocation
# parameters:
# Return location of last point in snapshot

sub getEndPointLocation {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getEndPointLocation",1);

    my $res;
    #my $operation = "resources/json/delphix/snapshot/" . $reference . "/timeflowRange" ;

    if (defined($self->{_snapshots}->{$reference}->{timeflowRange}) ) {
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    } else {
        $self->getTimeflowRange($reference);
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
    }

    my $ts = defined($res->{endPoint}) ? $res->{endPoint}->{location} : undef ;
    return defined($ts) ? $ts : 'N/A';
}


# Procedure getTimeflowRange
# parameters:
# Return location of last point in snapshot

sub getTimeflowRange {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getTimeflowRange",1);


    #my $operation = "resources/json/delphix/snapshot/" . $reference . "/timeflowRange" ;

    my $operation;
    if (defined($self->{_traverseTimeflows})) {
        $operation = "resources/json/delphix/snapshot/" . $reference . "/timeflowRange?traverseTimeflows=true";
    } else {
        $operation = "resources/json/delphix/snapshot/" . $reference  . "/timeflowRange"
    }

    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK') && defined($result->{result})) {
        $self->{_snapshots}->{$reference}->{timeflowRange} = $result->{result};
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
        $self->{_snapshots}->{$reference}->{timeflowRange} = 'N/A';
    }

}

# Procedure setRetention
# parameters:
# - referantion - snapshot ref
# - retation time in days -1 forever
# Return 0 if OK

sub setRetention {
    my $self = shift;
    my $reference = shift;
    my $retention = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::setRetention",1);

    my %setsnap = (
        "type" => $self->getSnapshotType($reference),
        "retention" => $retention
    );

    my $json_data = to_json(\%setsnap);

    my $operation = 'resources/json/delphix/snapshot/' . $reference;

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ($result->{status} eq 'OK') {
        print "Snapshot " . $self->getSnapshotName($reference)  . " updated\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure deleteSnapshot
# parameters:
# - referantion - snapshot ref
# Return 0 if OK

sub deleteSnapshot {
    my $self = shift;
    my $reference = shift;
    my $retention = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::deleteSnapshot",1);


    my $operation = 'resources/json/delphix/snapshot/' . $reference . "/delete";

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, '{}');

    if ($result->{status} eq 'OK') {
        print "Snapshot " . $self->getSnapshotName($reference)  . " deleted\n";
        return 0;
    } else {
        print "Snapshot not deleted due to error: " . $result->{error}->{details} . "\n" ;
        return 1;
    }

}

# Procedure getSnapshotSize
# parameters:
# - ref
# Return size of snapshot ref in bytes

sub getSnapshotSize {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotSize",1);


    my @snapshotarray = ( $reference );
    my $operation = "resources/json/delphix/snapshot/space";
    my %snapshot_hash = (
      "type" => "SnapshotSpaceParameters",
      "objectReferences" => \@snapshotarray
    );
    my $snapjson = to_json(\%snapshot_hash);

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $snapjson);

    if ($result->{status} eq 'OK') {
      return $result->{result}->{totalSize};
    } else {
      print "Snapshot not found " . $result->{error}->{details} . "\n" ;
      return undef;
    }

}

# Procedure getVDBTimezone
# parameters:
# Load 1 snapshot object for a database from Delphix Engine

sub getVDBTimezone
{
    my $self = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getVDBTimezone",1);

    my $timezone_op = "resources/json/delphix/snapshot?pageSize=1&database=" . $self->{_container};
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($timezone_op);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        if (scalar(@res) > 0) {
            $self->{_snapshots}->{$res[-1]->{reference}} = $res[-1];
            $self->{_timezone} = $self->getSnapshotTimeZone($res[-1]->{reference});
            delete $self->{_snapshots}->{$res[-1]->{reference}};
        }
    } else {
        print "Can't check snapshot timezone \n";
        exit 1;
    }
    if (defined($self->{_timezone})) {
      return $self->{_timezone};
    } else {
      return "N/A";
    }
}

# Procedure getSnapshotList
# parameters: - none
# Load snapshot objects from Delphix Engine

sub getSnapshotList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotList",1);
    my $operation = "resources/json/delphix/snapshot";

    my $operationroot;

    if (defined($self->{_container})) {
        if (defined($self->{_traverseTimeflows})) {
            $operation = $operation . "?database=" . $self->{_container} . "&traverseTimeflows=true";
        } else {
            $operation = $operation . "?database=" . $self->{_container} ;
        }

        if (defined($self->{_startDate}) || defined($self->{_endDate})  ) {
            # timezone check
            $self->getVDBTimezone();
        }

        if (defined($self->{_startDate}) && defined($self->{_timezone}) ) {
            my $startDate = Toolkit_helpers::convert_to_utc($self->{_startDate}, $self->{_timezone},0,1);
            if (defined($startDate)) {
                $operation = $operation . "&fromDate=" . $startDate;
            } else {
                print "Can't parse or convert start date to GMT\n";
                exit 1;
            }
        }

        $operationroot = $operation;

        if (defined($self->{_endDate}) && defined($self->{_timezone}) ) {
            my $endDate = Toolkit_helpers::convert_to_utc($self->{_endDate}, $self->{_timezone},0,1);
            if (defined($endDate)) {
                $operation = $operation . "&toDate=" . $endDate
            } else {
                print "Can't convert end date to GMT\n";
                exit 1;
            }
        }

    } else {
      $operation = $operation . "?";
      $operationroot = $operation;
    }

    # start pagination

    my $pagesize = 100;
    my $total;
    my $sofar = 0;
    my $pageoffset;

    my @snapshot_order;
    my $pageloop = 1;

    while ( $pageloop ) {

      if ($sofar == 0) {
        $operation = $operation . "&pageSize=" . $pagesize;
      } else {
        $operation = $operationroot . "&pageSize=" . $pagesize . "&toDate=" . $pageoffset;
      }

      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
      $total = $result->{total};

      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
          # total is not workig like expected with filters
          # this will stop loop if page is not returning pagesize objects
          if (scalar(@{$result->{result}}) == $pagesize) {
            $sofar = $sofar + scalar(@{$result->{result}});
          } else {
            $sofar = $sofar + scalar(@{$result->{result}});
            $pageloop = 0;
          }

          if ( scalar(@{$result->{result}}) ) {

              my @res = @{$result->{result}};

              my $snapshots = $self->{_snapshots};

              for my $snapitem (@res) {
                  $snapshots->{$snapitem->{reference}} = $snapitem;
                  #push(@snapshot_order, $snapitem->{reference});
                  unshift @snapshot_order, $snapitem->{reference};
              }

              $pageoffset = $self->{_snapshots}->{$snapshot_order[0]}->{latestChangePoint}->{timestamp};

              $self->{_snapshot_list} = \@snapshot_order;
              $self->{_snapshots} = $snapshots;
          }
      } else {
          print "No data returned for $operation. Try to increase timeout \n";
          exit 1;
      }
    }

}


# Procedure getSnapshotPerRef
# parameters:
# - snapshot_ref - load single snapshot
# Load single snapshot object from Delphix Engine

sub getSnapshotPerRef
{
    my $self = shift;
    my $snapshot_ref = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotPerRef",1);
    my $operation = "resources/json/delphix/snapshot/" . $snapshot_ref;

    my $operationroot;


    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my $res = $result->{result};
        $self->{_snapshots}->{$res->{reference}} = $res;
    } else {
        print "Can't load snapshot \n";
        return 1;
    }

}

1;
