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
# Program Name : Storage_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - System_obj - class which map a Delphix Engine Storage API object
# Author       : Marcin Przepiorowski
# Created      : 02 Sep 2016 (v2.0.0)
#
#



package Storage_obj;

use warnings;
use strict;
use Data::Dumper;
use Date::Manip;
use JSON;
use Toolkit_helpers qw (logger);

# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $debug = shift;
    logger($debug, "Entering Storage_obj::constructor",1);

    my %storage_test;
    my $self = {
        _dlpxObject => $dlpxObject,
        _debug => $debug,
        _storage_test => \%storage_test
    };
    
    bless($self,$classname);
    
    $self->LoadStorageTest();
    return $self;
}

# Procedure getTestList
# parameters: none
# Get a list of all storage tests

sub getTestList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Storage_obj::getTestList",1);
    my @ret = sort ( keys %{$self->{_storage_test}} );
    return \@ret;
}


# Procedure isTestExist
# parameters: 
# - reference
# Check if test exist 

sub isTestExist
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Storage_obj::isTestExist",1);
    return $self->{_storage_test}->{$reference};
}

# Procedure parseTestResults
# parameters: 
# - reference
# Build a hash of test results with test name as key

sub parseTestResults
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Storage_obj::parseTestResults",1);
    
    my $storage_tests = $self->{_storage_test};
    my %test_results_hash;
    
    if (defined($storage_tests->{$reference})) {
      if (defined($storage_tests->{$reference}->{testResults})) { 
        for my $testitem ( @{$storage_tests->{$reference}->{testResults}} ) {
          $test_results_hash{$testitem->{testName}} = $testitem;
        }  
      }
    }
    
    $storage_tests->{$reference}->{_test_results_hash} = \%test_results_hash;    
}

# Procedure getLatencyGrade
# parameters: 
# - reference
# - type
# Get a latency grade for test refrence and test type

sub getLatencyGrade
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getLatencyGrade",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{latencyGrade};
        }
    }
    
    return $ret;
}

# Procedure getLatencyMin
# parameters: 
# - reference
# - type
# Get a minimum latency for test refrence and test type

sub getLatencyMin
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getLatencyMin",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{minLatency};
        }
    }
    
    return $ret;
}

# Procedure getLatencyMax
# parameters: 
# - reference
# - type
# Get a maximum latency for test refrence and test type

sub getLatencyMax
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getLatencyMax",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{maxLatency};
        }
    }
    
    return $ret;
}

# Procedure getLatencyMax
# parameters: 
# - reference
# - type
# Get a average latency for test refrence and test type

sub getLatencyAvg
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getLatencyAvg",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{averageLatency};
        }
    }
    
    return $ret;
}

# Procedure getLatency95
# parameters: 
# - reference
# - type
# Get a 95 percentile latency for test refrence and test type

sub getLatency95
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getLatency95",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{latency95thPercentile};
        }
    }
    
    return $ret;
}

# Procedure getLatency95
# parameters: 
# - reference
# - type
# Get a 95 percentile latency for test refrence and test type

sub getLatencyStdDev
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getLatencyStdDev",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{stddevLatency};
        }
    }
    
    return $ret;
}

# Procedure getTestIOPS
# parameters: 
# - reference
# - type
# Get a IOPS for test refrence and test type

sub getTestIOPS
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getTestIOPS",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        #print Dumper $test_results->{}
        if (defined($test_results->{$type})) {
          $ret = $test_results->{$type}->{iops};
        }
    }
    
    return $ret;
}

# Procedure getTestThoughput
# parameters: 
# - reference
# - type
# Get a throughput for test refrence and test type

sub getTestThoughput
{
    my $self = shift;
    my $reference = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Storage_obj::getTestThoughput",1);
    
    my $ret;
    my $storage_tests = $self->{_storage_test};
    if (defined($storage_tests->{$reference})) {
        my $test_results = $storage_tests->{$reference}->{_test_results_hash};
        if (defined($test_results->{$type})) {
          $ret = sprintf("%10.2f",$test_results->{$type}->{throughput}/1024/1024);
        }
    }
    
    return $ret;
}

# Procedure getStartTime
# parameters: 
# - reference
# Get a state of test pointed by reference

sub getStartTime
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Storage_obj::getStartTime",1);
    
    my $storage_tests = $self->{_storage_test};
    
    my $ret;
    if (defined($storage_tests->{$reference})) {
        my $zulustart = $storage_tests->{$reference}->{startTime};
        my $tz = new Date::Manip::TZ;
        my $dt = new Date::Manip::Date;
        my $timezone = $self->{_dlpxObject}->getTimezone();
        $dt->config("setdate","zone,GMT");
        $zulustart =~ s/T/ /;
        $zulustart =~ s/\.000Z//;
        if ($dt->parse($zulustart)) {
          $ret = 'N/A';
        } else {
          my $dttemp = $dt->value();
          my ($err,$date,$offset,$isdst,$abbrev) = $tz->convert_from_gmt($dttemp, $timezone);
          $ret = sprintf("%04.4d-%02.2d-%02.2d %02.2d:%02.2d:%02.2d %s",$date->[0],$date->[1],$date->[2],$date->[3],$date->[4],$date->[5], $abbrev);
        }
    }
    
    
    
    
    return $ret;
}

# Procedure getState
# parameters: 
# - reference
# Get a state of test pointed by reference

sub getState
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Storage_obj::getState",1);
    
    my $storage_tests = $self->{_storage_test};
    
    my $ret;
    if (defined($storage_tests->{$reference})) {
        $ret = $storage_tests->{$reference}->{state};
    }
    
    return $ret;
}

# Procedure generateIORC
# parameters: 
# - reference
# - filename
# Generate a IORC for test 

sub generateIORC
{
    my $self = shift;
    my $reference = shift;
    my $filename = shift;
    logger($self->{_debug}, "Entering Storage_obj::generateIORC",1);
    my $operation = "resources/json/delphix/storage/test/" . $reference  . "/result";
    
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, '{}');
    my $ret;
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
      my $iorctext = $result->{result};

      my $file_handler;
      if (open($file_handler, '>' , $filename)) {
        print $file_handler $iorctext;
        close $file_handler;
        $ret = 0;
      } else {
        print "Can't open file $filename\n";
        $ret = 1;
      }

      

    } else {
      print "No data returned for $operation. Try to increase timeout \n";
      $ret = 1;
    }

    return $ret;
    
}


# Procedure LoadSystem
# parameters: none
# Load a list of System objects from Delphix Engine

sub LoadStorageTest
{
    my $self = shift;
    logger($self->{_debug}, "Entering Storage_obj::LoadStorageTest",1);
    my $operation = "resources/json/delphix/storage/test";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
      my @res = @{$result->{result}};
      
      my $storage_tests = $self->{_storage_test};

      for my $testitem (@res) {
          $storage_tests->{$testitem->{reference}} = $testitem;
      }
    } else {
      print "No data returned for $operation. Try to increase timeout \n";
    }


    
}

1;