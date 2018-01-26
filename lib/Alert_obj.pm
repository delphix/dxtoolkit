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
# Program Name : Action_obj.pm
# Description  : Delphix Engine Action object
# It's include the following classes:
# - Action_obj - class which map a Delphix Engine action API object
# Author       : Marcin Przepiorowski
# Created      : 20 Sep 2016 (v2.X.X)
#
#


package Alert_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use Date::Manip;
use lib '../lib';
use Jobs_obj;


# constructor
# parameters 
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $startTime = shift;
    my $endTime = shift;
    my $debug = shift;
    logger($debug, "Entering Alert_obj::constructor",1);

    my %alerts;
    my $self = {
        _alerts => \%alerts,
        _dlpxObject => $dlpxObject,
        _startTime => $startTime,
        _endTime => $endTime,
        _debug => $debug
    };
    
    bless($self,$classname);
    my $detz = $dlpxObject->getTimezone();
    $self->{_timezone} = $detz;
    $self->loadAlertList();
    return $self;
}

# Procedure getEventTitle
# parameters: 
# - reference
# Return event title for alert

sub getEventTitle {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getEventTitle",1);    
    my $alert = $self->{_alerts}->{$reference};
    
    my $ret;
        
    if (defined($alert->{eventTitle})) {
      $ret = $alert->{eventTitle};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getEventDesc
# parameters: 
# - reference
# Return event title for alert

sub getEventDesc {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getEventDesc",1);    
    my $alert = $self->{_alerts}->{$reference};
    
    my $ret;
        
    if (defined($alert->{eventDescription})) {
      $ret = $alert->{eventDescription};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getEventSeverity
# parameters: 
# - reference
# Return event serverity for alert

sub getEventSeverity {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getEventSeverity",1);    
    my $alert = $self->{_alerts}->{$reference};
    
    my $ret;
        
    if (defined($alert->{eventSeverity})) {
      $ret = $alert->{eventSeverity};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getEventResponse
# parameters: 
# - reference
# Return event response for alert

sub getEventResponse {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getEventResponse",1);    
    my $alert = $self->{_alerts}->{$reference};
    
    my $ret;
        
    if (defined($alert->{eventResponse})) {
      $ret = $alert->{eventResponse};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}

# Procedure getEventAction
# parameters: 
# - reference
# Return event action for alert

sub getEventAction {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getEventAction",1);    
    my $alert = $self->{_alerts}->{$reference};
    
    my $ret;
        
    if (defined($alert->{eventAction})) {
      $ret = $alert->{eventAction};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}



# Procedure getTargetName
# parameters: 
# - reference
# Return Target name for alert

sub getTargetName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getTargetName",1);    
    my $alert = $self->{_alerts}->{$reference};
    
    my $ret;
        
    if (defined($alert->{targetName})) {
      $ret = $alert->{targetName};
    } else {
      $ret = 'N/A';
    }
    return $ret;
}


# Procedure getTimeStampWithTZ
# parameters: 
# - reference
# Return alert date with engine time zone

sub getTimeStampWithTZ {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Alert_obj::getTimeStampWithTZ",1);    
    my $alerts = $self->{_alerts}->{$reference};
    my $ts = $alerts->{timestamp};
    return Toolkit_helpers::convert_from_utc($ts, $self->{_timezone}, 1);
}

# Procedure getAlertList
# - sort order asc / desc
# Return list of all events ordered by alert id

sub getAlertList {
    my $self = shift;
    my $order = shift;


    logger($self->{_debug}, "Entering Alert_obj::getActionList",1);    

    my $alerts = $self->{_alerts};
    my @ret;
    my @alerts_list;

    @alerts_list = keys %{$alerts};

    if ( (defined($order)) && (lc $order eq 'desc' ) ) {
        @ret = sort  { Toolkit_helpers::sort_by_number($b, $a) } ( @alerts_list );
    } else {
        @ret = sort { Toolkit_helpers::sort_by_number($a, $b) } ( @alerts_list );
    }

    return \@ret;
}




# Procedure loadActionList
# parameters: none
# Load a list of role objects from Delphix Engine

sub loadAlertList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Alert_obj::loadActionList",1);   
    my $pageSize = 5000;
    my $offset = 0;
    my $operation = "resources/json/delphix/alert?pageSize=$pageSize&pageOffset=$offset";

    if ($self->{_startTime}) {
        $operation = $operation . "&fromDate=" . $self->{_startTime};
    }
    
    if ($self->{_endTime}) {
        $operation = $operation . "&toDate=" . $self->{_endTime};
    }

    my $total = 1;
    
    my $alerts = $self->{_alerts};
    
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

        for my $alertitem (@res) {
            $alerts->{$alertitem->{reference}} = $alertitem;
        } 

      } else {
          print "No data returned for $operation. Try to increase timeout \n";
      }
    }
}

1;