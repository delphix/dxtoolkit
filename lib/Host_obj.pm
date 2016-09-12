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
# Program Name : Host_obj.pm
# Description  : Delphix Engine Source object
# It's include the following classes:
# - Host_obj - class which map a Delphix Engine host API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#


package Host_obj;

use warnings;
use strict;
use Data::Dumper;
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
    logger($debug, "Entering Host_obj::constructor",1);

    my %hosts;
    my $self = {
        _hosts => \%hosts,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->loadHostList($debug);
    return $self;
}


# Procedure getAllHosts
# parameters:
# none
# Return host all hosts refrences

sub getAllHosts {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Host_obj::getAllHosts",1);

    my $hosts = $self->{_hosts};
    
    return sort (keys %{$hosts});
    
}

# Procedure getHost
# parameters:
# - reference - reference of host
# Return host hash for specific host reference

sub getHost {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Host_obj::getHost",1);

    my %nahost = (
        "name" => "NA"
    );

    my $hosts = $self->{_hosts};
    my $ret;

    if (defined($reference) && defined($hosts->{$reference}) ) {
        $ret = $hosts->{$reference};
    } else {
        $ret = \%nahost;
    }
    return $ret;
}


# Procedure getHostByAddr
# parameters:
# - address - address of host
# Return host reference for specific host address

sub getHostByAddr {
    my $self = shift;
    my $address = shift;

    logger($self->{_debug}, "Entering Host_obj::getHostByAddr",1);

    my $hosts = $self->{_hosts};
    my $host_ref;

    for my $hostitem (keys %{$hosts}) {
      if ($hosts->{$hostitem}->{name} eq $address) {
        $host_ref = $hostitem;
        last;
      };
    }

    return $host_ref;
}

# Procedure getHostAddr
# parameters:
# - reference - reference of host
# Return host hash for specific host reference

sub getHostAddr {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Host_obj::getHostAddr",1);


    my $hosts = $self->{_hosts};
    my $ret;

    if (defined($reference) && defined($hosts->{$reference}) ) {
        $ret = $hosts->{$reference}->{name};
    } else {
        $ret = 'NA';
    }
    return $ret;
}

# Procedure getTimezone
# parameters:
# - reference - reference of host
# Return timezone for host ref

sub getTimezone {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Host_obj::getTimezone",1);
    return $self->{_hosts}->{$reference}->{hostConfiguration}->{operatingSystem}->{timezone};
}



# Procedure getToolkitpath
# parameters:
# - reference - reference of host
# Return toolkit path for reference

sub getToolkitpath {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Host_obj::getToolkitpath",1);
    return $self->{_hosts}->{$reference}->{toolkitPath};
}

# Procedure loadHostList
# parameters: - none
# Load list of host objects from Delphix Engine

sub loadHostList
{
    my $self = shift;

    logger($self->{_debug}, "Entering Host_obj::loadHostList",1);
    my $operation = "resources/json/delphix/host";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $hosts = $self->{_hosts};
        for my $hostitem (@res) {
            $hosts->{$hostitem->{reference}} = $hostitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;
