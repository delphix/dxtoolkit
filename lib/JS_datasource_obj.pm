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
# Program Name : JS_datasource_obj.pm
# Description  : Delphix Engine JS datasource
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#
#


package JS_datasource_obj;

use warnings;
use strict;
use Data::Dumper;
use Date::Manip;
use JSON;
use version;
use Toolkit_helpers qw (logger);

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $template = shift;
    my $container = shift;
    my $debug = shift;
    logger($debug, "Entering JS_datasource_obj::constructor",1);

    my %jsdatasource;
    my $self = {
        _jsdatasource => \%jsdatasource,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->loadJSDataSourceList($template, $container, $debug);
    return $self;
}


# Procedure getJSDataSourceByName
# parameters:
# - name
# Return template reference for particular name

sub getJSDataSourceByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering JS_datasource_obj::getJSDataSourceByName",1);
    my $ret;

    #print Dumper $$config;

    for my $dsitem ( sort ( keys %{$self->{_jsdatasource}} ) ) {

        if ( $self->getName($dsitem) eq $name) {
            $ret = $dsitem;
        }
    }

    return $ret;
}


# Procedure getJSDataSourceByContainer
# parameters:
# - conref
# Return data source reference for particular container

sub getJSDataSourceByContainer {
    my $self = shift;
    my $conref = shift;
    logger($self->{_debug}, "Entering JS_datasource_obj::getJSDataSourceByContainer",1);
    my @retarray;

    #print Dumper $$config;

    for my $dsitem ( sort ( keys %{$self->{_jsdatasource}} ) ) {

        if ( $self->getJSDataLayout($dsitem) eq $conref) {
            push(@retarray, $dsitem);
        }
    }

    return \@retarray;
}

# Procedure getJSDataSource
# parameters:
# - reference
# Return template hash for specific template reference

sub getJSDataSource {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getJSDataSource",1);

    my $jsdatasource = $self->{_jsdatasource};
    return $jsdatasource->{$reference};
}


# Procedure getJSDataSourceList
# parameters:
# Return JS datasource list

sub getJSDataSourceList {
    my $self = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getJSDataSourceList",1);

    my @arrret = sort (keys %{$self->{_jsdatasource}} );

    return \@arrret;
}


# Procedure getName
# parameters:
# - reference
# Return JS template name for specific template reference

sub getName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getName",1);

    my $jsdatasource = $self->{_jsdatasource};
    return $jsdatasource->{$reference}->{name};
}

# Procedure getJSTemplate
# parameters:
# - reference
# Return JS template for specific Data Source reference

sub getJSTemplate {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getJSTemplate",1);

    my $jsdatasource = $self->{_jsdatasource};
    return $jsdatasource->{$reference}->{dataLayout};
}


# Procedure getJSDBContainer
# parameters:
# - reference
# Return JS DB container for specific Data Source reference

sub getJSDBContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getJSDBContainer",1);

    my $jsdatasource = $self->{_jsdatasource};
    return $jsdatasource->{$reference}->{container};
}

# Procedure getJSDataLayout
# parameters:
# - reference
# Return JS datasource container

sub getJSDataLayout {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getJSDataLayout",1);

    my $jsdatasource = $self->{_jsdatasource};
    return $jsdatasource->{$reference}->{dataLayout};
}

# Procedure getProperties
# parameters:
# - reference
# Return JS template properties hash for specific template reference

sub getProperties {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::getProperties",1);

    my $jsdatasource = $self->{_jsdatasource};
    return $jsdatasource->{$reference}->{properties};
}


# Procedure checkTime
# parameters:
# - reference
# Return array of datasource times for specific datasource reference and time

sub checkTime {
    my $self = shift;
    my $reference = shift;
    my $time = shift;
    my $noformat = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::checkTime",1);

    my %checktime_hash;
    if (version->parse($self->{_dlpxObject}->getApi()) < version->parse(1.8.0)) {
      %checktime_hash = (
          "type" => "JSSourceDataTimestampParameters",
          "dataLayout" => $reference,
          "time" => $time
        );
    } else {
      %checktime_hash = (
          "type" => "JSSourceDataTimestampParameters",
          "branch" => $reference,
          "time" => $time
        );
    }

    my $json_data = to_json(\%checktime_hash, {pretty=>1});

    my $operation = "resources/json/delphix/jetstream/datasource/dataTimestamps";

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        my @source_time;
        my $tz = new Date::Manip::TZ;
        my $detz = $self->{_dlpxObject}->getTimezone();
        for my $t ( @{$result->{result} } ) {
          if (defined($noformat)) {
            my %source_hash = (
              'name' => $t->{name},
              'timestamp' => $t->{timestamp},
              'dsref' => $t->{source}
            );
            push(@source_time, \%source_hash);
          } else {
            $t->{timestamp} =~ s/\....Z//;
            my $dt = ParseDate($t->{timestamp});
            my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dt, $detz);
            my %source_hash = (
              'name' => $t->{name},
              'dsref' => $t->{source},
              'timestamp' => sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev)
            );
            push(@source_time, \%source_hash);
          }
        }

        #return $result->{result};
        return \@source_time;
    } else {
        return undef;
    }
}


# Procedure checkTime
# parameters:
# - reference
# Return array of datasource times for specific datasource reference and time

sub checkTimeDelta {
    my $self = shift;
    my $reference = shift;
    my $time = shift;
    my $diff = shift;

    logger($self->{_debug}, "Entering JS_datasource_obj::checkTime",1);

    my %checktime_hash = (
        "type" => "JSSourceDataTimestampParameters",
        "dataLayout" => $reference,
        "time" => $time
    );

    my $ret = 0;

    my $json_data = to_json(\%checktime_hash, {pretty=>1});

    my $operation = "resources/json/delphix/jetstream/datasource/dataTimestamps";

    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {

        my @source_time;
        my $tz = new Date::Manip::TZ;
        my $detz = $self->{_dlpxObject}->getTimezone();
        my %source_hash ;
        for my $t ( @{$result->{result} } ) {

            # print Dumper $t->{timestamp};
            # print Dumper "DELTA";
            my $time_epoch = UnixDate($time,'%s');
            my $realtime_epoch = UnixDate($t->{timestamp},'%s');
            # print Dumper $time_epoch;
            # print Dumper $realtime_epoch;
            my $delta = $time_epoch - $realtime_epoch + 0;
            # print Dumper $delta;
            # print Dumper $diff;
            # print Dumper ($delta >= $diff);
            if ($delta  >= $diff) {
                $ret = $ret + 1;
            }
        }
    }

    return $ret;

}


# Procedure loadJSDataSourceList
# parameters: none
# Load a list of template objects from Delphix Engine

sub loadJSDataSourceList
{
    my $self = shift;
    my $template = shift;
    my $container = shift;
    logger($self->{_debug}, "Entering JS_datasource_obj::loadJSDataSourceList",1);

    my $operation = "resources/json/delphix/jetstream/datasource?";

    if (defined($template)) {
        $operation = $operation . "dataLayout=" . $template . "&";
    }

    if (defined($container)) {
        $operation = $operation . "container=" . $container . "&";
    }

    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jsdatasource = $self->{_jsdatasource};

        for my $dsitem (@res) {
            $jsdatasource->{$dsitem->{reference}} = $dsitem;
        }
    }
}

1;
