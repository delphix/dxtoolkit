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
    my $timezone = shift;
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
        _timezone => $timezone
    };
    
    bless($self,$classname);
    
    $self->getSnapshotList($debug);
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

# Procedure getContainer
# parameters: refrerence
# Return cointainer for reference

sub getLatestPoint {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getFirstPoint",1);   

    return defined($self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp}) ? $self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp} : 'N/A';
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
    return $self->{_snapshots}->{$reference}->{name};
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
        my $tz = new Date::Manip::TZ;
        $zulutime =~ s/T//;
        $zulutime =~ s/...Z$//;
        
        #print Dumper $zulutime;

        my $dt = ParseDate($zulutime);
        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $timezone);
        if (scalar(@{$date}) > 0) {
            $ret = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
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
    my $ts = $self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp};
    
    # if $ts is null - I need to reconsider which one to use - latest change of snapshot process seems OK  
    #print Dumper $self->{_snapshots}->{$reference}->{latestChangePoint}->{timestamp};
    #print Dumper $self->{_snapshots}->{$reference}->{creationTime};
    #print Dumper $self->{_snapshots}->{$reference}->{name};
    chomp($ts); 
    $ts =~ s/T/ /;
    $ts =~ s/\....Z//;
    return $ts;
}

# Procedure checkTZ 
# parameters: 
# - timezone
# Return 0 if OK

sub checkTZ {
    my $self = shift;
    my $timezone = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::checkTZ",1); 

    my $checktime = time();
    my $dt = ParseDate($checktime);
    my $tz = new Date::Manip::TZ;
    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $timezone);

    if (defined($abbrev)) { 
        logger($self->{_debug}, "checkTZ abbrev-" . $abbrev ,1);
    } else {
        logger($self->{_debug}, "checkTZ abbrev-undefined" ,1);
    }

    return $err;

}

# Procedure getSnapshotTimeZone
# parameters: 
# - reference
# Return timestamp for snapshot

sub getSnapshotTimeZone {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotTimeZone",1); 
    my $ts = $self->{_snapshots}->{$reference}->{timezone} ;
    logger($self->{_debug}, "Snapshot timezone returned by DE $ts",1); 
    chomp($ts); 
    my @temp = split(',',$ts);
    my $ret = $temp[0];
    if ($ret eq 'Etc/Zulu') {
        $ret = 'Etc/GMT';
    }

    my $tz = new Date::Manip::TZ;

    my @zone = ('Etc/GMT');
    my ($err,$val) = $tz->define_offset('+0000', @zone);

    logger($self->{debug}, "Setting GMT timezone err-" . $err );

    @zone = ('Asia/Singapore');
    ($err,$val) = $tz->define_offset('+0800', @zone);    

    logger($self->{debug}, "Setting SGT timezone err-" . $err );
    
    # checking if timezone if valid 
    if ($self->checkTZ($ret)) {

        # can't resolve time zone
        # try to use offset
                
        my $offset = $temp[1];
        if ( (my ($tzoff) = $offset =~ /[a-zA-Z]{3}(.\d\d\d\d)/ ) ) {
            $ret = $tz->zone($tzoff);
        } elsif ( ( ($tzoff) = $offset =~ /[a-zA-Z]{3}.\d\d:\d\d(.\d\d\d\d)/ ) ) {
            $ret = $tz->zone($tzoff);
        }  
        else {
            $ret = 'N/A';
        }

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
    my $timezone = $self->getSnapshotTimeZone($reference);

    if ($timezone eq 'N/A') {
        $ret = 'N/A - timezone unknown';
    } else {
        my $dt = ParseDate($zulutime);
        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $timezone);
        if (scalar(@{$date}) > 0) {
            $ret = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
        } else {
            $ret = 'N/A';
        }
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
    my $tz = new Date::Manip::TZ;
    my $dt = ParseDate($timestamp);


    my $match = 0;
    for my $snapitem ( @{$self->getSnapshots()} ) {

        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $self->getSnapshotTimeZone($snapitem));

        my $sttz = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);



        my $snap_startpoint = $self->getStartPoint($snapitem);
        my $snap_endpoint = $self->getEndPoint($snapitem);

        $snap_startpoint =~ s/T/ /;
        $snap_startpoint =~ s/\....Z//;
        $snap_endpoint =~ s/T/ /;
        $snap_endpoint =~ s/\....Z//;


        if  ( ($snap_startpoint le $sttz) && ($snap_endpoint ge $sttz ) ) {
            $match = $match + 1;
            $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
            $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
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
# Return timeflow for timestamp

sub findSnapshotforTimestamp {
    my $self = shift;
    my $timestamp = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::findSnapshotforTimestamp",1);  

    my %ret;
    my $tz = new Date::Manip::TZ;
    my $dt = ParseDate($timestamp);


    my $match = 0;
    for my $snapitem ( @{$self->getSnapshots()} ) {

        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $self->getSnapshotTimeZone($snapitem));

        my $sttz = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4]);

        my $snap_startpoint = $self->getStartPoint($snapitem);
        my $final_ts = $snap_startpoint;
        my $snap_endpoint = $self->getEndPoint($snapitem);

        $snap_startpoint =~ s/T/ /;
        $snap_startpoint =~ s/\:\d\d\.\d\d\dZ$//;
        $snap_endpoint =~ s/T/ /;
        $snap_endpoint =~ s/\:\d\d\.\d\d\dZ$//;


        if  ( ( ($snap_startpoint le $sttz) && ($snap_endpoint gt $sttz ) ) || ( ( $snap_startpoint eq $snap_endpoint ) && ($sttz eq $snap_startpoint) ) ) {
            $match = $match + 1;
            $ret{timeflow} = $self->getSnapshotTimeflow($snapitem);
            $ret{timezone} = $self->getSnapshotTimeZone($snapitem);
            $ret{timestamp} = $final_ts;
        }

    }

    if ($match gt 1) {
        print "Timestamp in more than one snapshot. Exiting\n";
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


    $zulutime =~ s/T/ /;
    $zulutime =~ s/\....Z//;
    my $timezone = $self->getSnapshotTimeZone($reference);
    my $dt = ParseDate($zulutime);
    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $timezone);
    return sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
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
        $self->getTimeflowRange($reference);
        $res = $self->{_snapshots}->{$reference}->{timeflowRange};
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

    $zulutime =~ s/T/ /;
    $zulutime =~ s/\....Z//;
    my $timezone = $self->getSnapshotTimeZone($reference);
    my $dt = ParseDate($zulutime);
    my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $timezone);
    return sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
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
        $self->getTimeflowRange($reference);
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

    if (defined($result->{result})) {
        $self->{_snapshots}->{$reference}->{timeflowRange} = $result->{result}
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


# Procedure getSnapshotList
# parameters: - none
# Load snapshot objects from Delphix Engine

sub getSnapshotList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Snapshot_obj::getSnapshotList",1);   
    my $operation = "resources/json/delphix/snapshot";



    if (defined($self->{_container})) {
        if (defined($self->{_traverseTimeflows})) {
            $operation = $operation . "?database=" . $self->{_container} . "&traverseTimeflows=true";      
        } else {
            $operation = $operation . "?database=" . $self->{_container} ;   
        }  
    
        if (defined($self->{_startDate}) || defined($self->{_endDate})  ) {
            # timezone check
            my $timezone_op = "resources/json/delphix/snapshot?pageSize=1&database=" . $self->{_container};
            my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($timezone_op);
            if (defined($result->{status}) && ($result->{status} eq 'OK')) {
                my @res = @{$result->{result}};
                if (scalar(@res) > 0) {
                    $self->{_snapshots}->{$res[-1]->{reference}} = $res[-1];
                    #print Dumper $self->{_snapshots};    
                    $self->{_timezone} = $self->getSnapshotTimeZone($res[-1]->{reference});
                    #print Dumper $self->{_timezone};
                    delete $self->{_snapshots}->{$res[-1]->{reference}};
                    #print Dumper $self->{_snapshots};  
                }
            } else {
                print "Can't check snapshot timezone \n";
                exit 1;
            }
        }

        if (defined($self->{_startDate}) && defined($self->{_timezone}) ) {

            my $tz = new Date::Manip::TZ;
            my $dt = ParseDate($self->{_startDate});

            if ($dt eq '') {
                print "Can't parse start date \n";
                exit 1;
            }

            my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $self->{_timezone});

            if (! $err) {
                $operation = $operation . "&fromDate=" . sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
            } else {
                print "Can't convert start date to GMT\n";
                exit 1;
            }
            
        }

        if (defined($self->{_endDate}) && defined($self->{_timezone}) ) {
            my $tz = new Date::Manip::TZ;
            my $dt = ParseDate($self->{_endDate});

            if ($dt eq '') {
                print "Can't parse end date \n";
                exit 1;
            }

            my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $self->{_timezone});

            if (! $err) {
                $operation = $operation . "&toDate=" . sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
            } else {
                print "Can't convert end date to GMT\n";
                exit 1;
            }
            
        }
    }

    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {

        if ( scalar(@{$result->{result}}) ) {

            my @res = @{$result->{result}};

            my $snapshots = $self->{_snapshots};

            my @snapshot_order;

            for my $snapitem (@res) {
                $snapshots->{$snapitem->{reference}} = $snapitem;
                push(@snapshot_order, $snapitem->{reference});
            } 

            $self->{_snapshot_list} = \@snapshot_order;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
        exit 1;
    }
}

1;