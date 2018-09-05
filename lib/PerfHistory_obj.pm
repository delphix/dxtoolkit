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
# Copyright (c) 2018 by Delphix. All rights reserved.
#
# Program Name : PerfHistory_obj.pm
# Description  : Delphix Engine performance history
# It's include the following classes:
# - PerfHistory_obj - class which map a Delphix Engine performance history API
# Author       : Marcin Przepiorowski
# Created      : 27 Aug 2018 (v2.3.7)
#


package PerfHistory_obj;

use warnings;
use strict;
use POSIX;
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
    my $startDate = shift;
    my $endDate = shift;
    my $sampling = shift;
    my $noofdbs = shift;
    my $debug = shift;
    logger($debug, "Entering PerfHistory_obj::constructor",1);


    my %timehash;
    my $self = {
        _timehash => \%timehash,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->getPerformance($startDate, $endDate, $sampling, $noofdbs);
    return $self;
}


sub returndata
{
    my $self = shift;
    return $self->{_timehash};
}

# Procedure pivotData
# parameters: - none
# Pivot data into timestamp, VDB, VDB, VDB format

sub pivotData
{
    my $self = shift;
    my $cp = shift;
    logger($self->{_debug}, "Entering PerfHistory_obj::pivotData",1);

    my %timehash;
    my $timestamp;

    for my $cont (keys(%{$cp})) {
      for my $item (@{$cp->{$cont}}) {
        $timestamp = $item->{"timestamp"};
        $timehash{$timestamp}{$cont} =  $item->{"averageThroughput"};
      }
    }

    return \%timehash;
}


# Procedure getPerformance
# parameters: - none
# Load performance objects from Delphix Engine

sub getPerformance
{
    my $self = shift;
    my $startDate = shift;
    my $endDate = shift;
    my $sampling = shift;
    my $noofdbs = shift;
    logger($self->{_debug}, "Entering PerfHistory_obj::getTimeflowList",1);

    my $interval;
    # workaround for DLPX-56856
    my $enginelimit = 5000;

    if ($sampling == 3600) {
       $interval = "&samplingInterval=3600";
       $enginelimit = $enginelimit*3600-$noofdbs*3600;
    } elsif ($sampling == 60) {
       $interval = "&samplingInterval=60";
       $enginelimit = $enginelimit*60-$noofdbs*60;
    } elsif ($sampling == 1) {
       $interval = "&samplingInterval=1";
    } else {
      print "Wrong sampling size $sampling\n";
      return undef;
    }

    # number of datapoint need to be below $enginelimit
    # so number of objects matter
    my $numberofsec = floor($enginelimit/$noofdbs);
    logger($self->{_debug}, "sampling $sampling",2);
    logger($self->{_debug}, "enginelimit $enginelimit",2);
    logger($self->{_debug}, "Number of sec $numberofsec",2);
    my $localenddate;
    my $operation;
    my $deltadate;
    my $localstartdate;
    my $stop = 0;

    # 26 hours check
    my $maxstartdate = Toolkit_helpers::convert_to_utc($self->{_dlpxObject}->getTime(1560),'UTC',undef,1);

    if ($maxstartdate gt $startDate) {
      $localstartdate = $maxstartdate;
    } else {
      $localstartdate = $startDate;
    }

    # looping through data
    while ($stop == 0) {
      # calculate localenddate keeping in mind data point limit
      $deltadate = DateCalc(ParseDate($localstartdate), ParseDateDelta('+ ' . $numberofsec . ' second'));
      $localenddate = Toolkit_helpers::convert_to_utc($deltadate,'UTC',undef,1);
      if ($localenddate ge $endDate) {
        $localenddate = $endDate;
        $stop = 1;
      }

      $operation = "resources/json/delphix/database/performanceHistory?fromDate=" . $localstartdate . "&toDate=" . $localenddate;
      $operation = $operation . $interval;

      my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
      my $allpoints = 0;
      if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my %cp;
        for my $contperf (@res) {
          if (!$contperf->{deleted}) {
            # put only objects which are not deleted
            $cp{$contperf->{container}} = $contperf->{"utilization"};
            $allpoints = $allpoints + scalar(@{$contperf->{"utilization"}});
          }
        }

        # convert data into timestamp, obj, ... format and keep it for all loop interation
        my $timehash = $self->pivotData(\%cp);
        my %merge = (%{$self->{_timehash}}, %{$timehash});
        $self->{_timehash} = \%merge;

        if (%cp) {
          # due to login inside engine
          # last data point can be below $localenddate
          # and to keep all seconds we need to change $localstartdate
          # to a real max returned timestamp
          $localstartdate = (sort(keys(%{$timehash})))[-1];
        } else {
          # to data returned - moving startdate to calculated enddate
          $localstartdate = $localenddate;
        }

        logger($self->{_debug}, "number of datapoint returned $allpoints",2);

      } else {
        print "No data returned for $operation. Try to increase timeout \n";
      }

    }

}



1;
