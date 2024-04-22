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
# Program Name : Bookmark_obj.pm
# Description  : Delphix Engine bookmark object
# It's include the following classes:
# - Environment_obj - class which map a Delphix Engine bookmark API object
# Author       : Marcin Przepiorowski
# Created      : 02 Jul 2015 (v2.0.0)
#


package Bookmark_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Date::Manip;
use Timeflow_obj;
use Snapshot_obj;
use Toolkit_helpers qw (logger);

# constructor
# parameters
# - dlpxObject - connection to DE
# - databases - only for create / list timestamps
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $databases = shift;
    my $debug = shift;
    logger($debug, "Entering Bookmark_obj::constructor",1);

    my %bookmarks;
    my $self = {
        _bookmarks => \%bookmarks,
        _dlpxObject => $dlpxObject,
        _databases => $databases,
        _debug => $debug
    };

    bless($self,$classname);

    my $timeflows = new Timeflow_obj ($self->{_dlpxObject}, undef, $debug);

    $self->{_timeflows} = $timeflows;

    $self->getBookmarkList($debug);
    return $self;
}


# Procedure getBookmarks
# parameters: none
# Return a list of bookmarks

sub getBookmarks
{
    my $self = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::getBookmarks",1);
    my @bookmarks = sort (keys %{$self->{_bookmarks}});

    return \@bookmarks;

}


# Procedure getBookmarkByName
# parameters:
# - name
# Return a bookmark hash or undef

sub getBookmarkByName
{
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::getBookmarkByName",1);

    my $ret;

    $ret = $self->{_bookmarks}->{$name};

    return $ret;
}


# Procedure getBookmarkTimeflow
# parameters:
# - name of bookmark
# Return a hash bookmark time / object name

sub getBookmarkTimeflow
{
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::getBookmarkTimeflow",1);

    my $timeflow_name;

    if (defined($name) && defined($self->{_bookmarks}->{$name}) ) {
        $timeflow_name = $self->{_bookmarks}->{$name}->{timeflow_name};
    } else {
        $timeflow_name = 'N/A';
    }

    return $timeflow_name;
}


# Procedure getBookmarkTimestamp
# parameters:
# - name of bookmark
# Return a hash bookmark time / object name

sub getBookmarkTimestamp
{
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::getBookmarkTimestamp",1);

    my %timestamp;


    if (defined($name) && defined($self->{_bookmarks}->{$name}) ) {
        my $bookmark_timestamp = $self->{_bookmarks}->{$name}->{timestamp};

        my $timeflow = $self->{_bookmarks}->{$name}->{timeflow};

        my $container = $self->{_timeflows}->getContainer($timeflow);

        if (defined($container)) {
            my $container_obj = $self->{_databases}->getDB($container);
            if (defined($container_obj)) {
                my $container_timezone = $container_obj->getTimezone();
                $bookmark_timestamp =~ s/\....Z//;
                $timestamp{timestamp} = Toolkit_helpers::convert_from_utc($bookmark_timestamp, $container_timezone,1);
                $timestamp{object_name} = $container_obj->getName();

            } else {
                $timestamp{timestamp} = 'N/A';
                $timestamp{object_name} = 'N/A';
            }
        } else {
            $timestamp{timestamp} = 'N/A';
            $timestamp{object_name} = 'N/A';
        }


    } else {
        $timestamp{timestamp} = 'N/A';
        $timestamp{object_name} = 'N/A';
    }



    return \%timestamp;
}




# Procedure createBookmark
# parameters:
# - db - database object
# - name - name of bookmark
# - time - if not specify - current time from engine is taken
# Create a bookmark for a container

sub createBookmark
{
    my $self = shift;
    my $db = shift;
    my $name = shift;
    my $time = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::createBookmark",1);

    if (defined($self->{_bookmarks}->{$name})) {
        print "Bookmark with name $name already exists.\n";
        return 1;
    }


    if (! defined($db)) {
        print "Database object not defined.\n";
        return 1;
    }
    my $db_reference = $db->getReference();

    my $current_timeflow = $self->{_timeflows}->getCurrentTimeflowForContainer($db_reference);

    my $bookmark_timeflow_type;

    if ($db->getDBType() eq 'oracle') {
        $bookmark_timeflow_type = 'OracleTimeflowPoint'
    } elsif ($db->getDBType() eq 'mssql') {
        $bookmark_timeflow_type = 'MSSqlTimeflowPoint'
    } elsif ($db->getDBType() eq 'sybase') {
        $bookmark_timeflow_type = 'ASETimeflowPoint'
    } elsif ($db->getDBType() eq 'mysql') {
        $bookmark_timeflow_type = 'MySQLTimeflowPoint'
    } elsif ($db->getDBType() eq 'appdata') {
        $bookmark_timeflow_type = 'AppDataTimeflowPoint'
    } elsif ($db->getDBType() eq 'vFiles') {
        $bookmark_timeflow_type = 'AppDataTimeflowPoint'
    } elsif ($db->getDBType() eq 'postgresql') {
        $bookmark_timeflow_type = 'AppDataTimeflowPoint'
    } else {
      print "Can't determine a DB type. Exiting\n";
      return 1;
    }


    my $temp_time;
    my $temp_timezone;
    my $bookmark_time;

    if (defined($time) && ( $time =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d?\d):(\d\d)/ ) ) {
        $temp_timezone = $db->getTimezone();
        $temp_time = $time;
        my $dt = ParseDate($temp_time);
        my $tz = new Date::Manip::TZ;
        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $temp_timezone);
        if ($err) {
            print "Can't set time for bookmark\n";
            return 1;
        } else {
            $bookmark_time = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
        }
    } elsif (defined($time) && (uc $time eq 'LATEST') ) {
        # this is to print time stamp of snapshot
        my $snapshots = new Snapshot_obj ($self->{_dlpxObject},$db_reference, 1, $self->{_debug});
        $snapshots->getSnapshotList($db_reference);
        ($temp_time,$temp_timezone) = $snapshots->getLatestSnapshotTime();
        my @timesplit = split(' ',$temp_time);

        $temp_time = $timesplit[0] . ' ' . $timesplit[1];
        my $tz = new Date::Manip::TZ;
        $temp_timezone = $tz->zone($temp_timezone);

        # bookmark has to be set to exact timestamp, so we need to call it again
        $bookmark_time = $snapshots->getLatestTime();


    } elsif (defined($time) && (uc $time eq 'NOW') ) {
        $temp_time = $self->{_dlpxObject}->getTime();
        $temp_timezone = $self->{_dlpxObject}->getTimezone();
        my $dt = ParseDate($temp_time);
        my $tz = new Date::Manip::TZ;
        my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_to_gmt($dt, $temp_timezone);
        if ($err) {
            print "Can't set time for bookmark\n";
            return 1;
        } else {
            $bookmark_time = sprintf("%04.4d-%02.2d-%02.2dT%02.2d:%02.2d:%02.2d.000Z",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5]);
            $temp_time = $bookmark_time;
        }
    } else {
        return 1;
    }









    my %bookmark_hash = (
        "type" => "TimeflowBookmarkCreateParameters",
        "name" => $name,
        "timeflowPoint" => {
            "type" => $bookmark_timeflow_type,
            "timeflow" => $current_timeflow,
            "timestamp" => $bookmark_time
        }
    );

    my $json_data = encode_json(\%bookmark_hash);


    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData('resources/json/delphix/timeflow/bookmark', $json_data);

    if (defined($result) && ($result->{status} eq 'OK' )) {
       $self->getBookmarkList();
       print "Bookmark $name for time $temp_time has been created\n";
       return 0;
    } else {
       print "Creatation of bookmark $name for time $temp_time failed\n";
       print $result->{error}->{details} . "\n";
       return 1;
    }




}


# Procedure deleteBookmark
# parameters:
# name
# Delete a bookmark

sub deleteBookmark
{
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::deleteBookmark",1);

    if (! defined($self->{_bookmarks}->{$name})) {
        print "Bookmark with name $name doesn't exists.\n";
        return 1;
    }

    my $bookmark = $self->getBookmarkByName($name);

    my $op = 'resources/json/delphix/timeflow/bookmark/' . $bookmark->{reference} . '/delete';

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($op, '{}');

    if (defined($result) && ($result->{status} eq 'OK' )) {
       $self->getBookmarkList();
       print "Bookmark $name has been deleted\n";
       return 0;
    } else {
       return 1;
    }




}


# Procedure getEnvironmentList
# parameters: none
# Load a list of bookmark objects from Delphix Engine

sub getBookmarkList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Bookmark_obj::getBookmarkList",1);

    my $operation = "resources/json/delphix/timeflow/bookmark";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $bookmarks = $self->{_bookmarks};

        my $name;
        my $timeflow_name;
        my $timeflow;

        for my $bookmarkitem (@res) {



            $name = $bookmarkitem->{name};
            $timeflow = $bookmarkitem->{timeflow};
            $timeflow_name = $self->{_timeflows}->getName($timeflow);

            if (! defined($timeflow_name)) {
                $timeflow_name = 'N/A';
            }

            if ($timeflow_name =~ /^JETSTREAM/) {
                next;
            } else {
                $bookmarkitem->{timeflow_name} = $timeflow_name;
                $bookmarks->{$name} = $bookmarkitem;
            }
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}


1;
