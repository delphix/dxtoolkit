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
# Program Name : Network_obj.pm
# Description  : Delphix Engine Network object
# It's include the following classes:
# - Network_obj - class which map a Delphix Engine Network API object
# Author       : Marcin Przepiorowski
# Created      : 11 Aug 2016 (v2.0.0)
#
#



package Network_obj;

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
    logger($debug, "Entering Network_obj::constructor",1);

    my %network_latency;
    my %network_thoughput;
    my %network_dsp;
    my $self = {
        _network_latency => \%network_latency,
        _network_throughput => \%network_thoughput,
        _network_dsp => \%network_dsp,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->loadLatencyTest();
    $self->loadThroughputTest();
    $self->loadDSPTest();

    return $self;
}

# Procedure getLatencyMax
# parameters: 
# - ref - network test refrerence
# Return a max latency from Delphix Engine

sub getLatencyMax
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyMax",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{maximum};
    return $ret;
}

# Procedure getLatencyMin
# parameters: 
# - ref - network test refrerence
# Return a min latency from Delphix Engine

sub getLatencyMin
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyMin",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{minimum};
    return $ret;
}

# Procedure getLatencyStdDev
# parameters: 
# - ref - network test refrerence
# Return a stddev latency from Delphix Engine

sub getLatencyStdDev
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyStdDev",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{stddev};
    return $ret;
}

# Procedure getLatencyCount
# parameters: 
# - ref - network test refrerence
# Return a latency count from Delphix Engine

sub getLatencyCount
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyCount",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{parameters}->{requestCount};
    return $ret;
}

# Procedure getLatencySize
# parameters: 
# - ref - network test refrerence
# Return a latency count from Delphix Engine

sub getLatencySize
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencySize",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{parameters}->{requestSize};
    return $ret;
}

# Procedure getLatencyLoss
# parameters: 
# - ref - network test refrerence
# Return a latency average from Delphix Engine

sub getLatencyLoss
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyLoss",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{loss};
    return $ret;
}

# Procedure getLatencyAvg
# parameters: 
# - ref - network test refrerence
# Return a latency average from Delphix Engine

sub getLatencyAvg
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyAvg",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{average};
    return $ret;
}


# Procedure getTestRate
# parameters: 
# - ref - network test refrerence
# Return a throughput from Delphix Engine

sub getTestRate
{
   my $self = shift;
   my $reference = shift;
   logger($self->{_debug}, "Entering Network_obj::getTestRate",1);

   my $ret;
   my $network;
     
   if (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
   } else {
      return 'N/A';
   }
    
   $ret = sprintf("%10.2f",$network->{throughput}/1024/1024);
   return $ret;
}

# Procedure getTestDirection
# parameters: 
# - ref - network test refrerence
# Return a test direction from Delphix Engine

sub getTestDirection
{
   my $self = shift;
   my $reference = shift;
   logger($self->{_debug}, "Entering Network_obj::getTestDirection",1);

   my $ret;
   my $network;
     
   if (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
   } else {
      return 'N/A';
   }
    
   $ret = $network->{parameters}->{direction};
   return $ret;
}

# Procedure getTestBlockSize
# parameters: 
# - ref - network test refrerence
# Return a test block size from Delphix Engine

sub getTestBlockSize
{
   my $self = shift;
   my $reference = shift;
   logger($self->{_debug}, "Entering Network_obj::getTestBlockSize",1);

   my $ret;
   my $network;
     
   if (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
   } else {
      return 'N/A';
   }
    
   $ret = $network->{parameters}->{blockSize};
   return $ret;
}

# Procedure getTestConnections
# parameters: 
# - ref - network test refrerence
# Return a test number of connection from Delphix Engine

sub getTestConnections
{
   my $self = shift;
   my $reference = shift;
   logger($self->{_debug}, "Entering Network_obj::getTestConnections",1);

   my $ret;
   my $network;
        
   if (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
   } else {
      return 'N/A';
   }
    
   $ret = $network->{numConnections};
   return $ret;
}

# Procedure getState
# parameters: 
# - ref - network test refrerence
# Return a test state of network tests from Delphix Engine

sub getState
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getState",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } elsif (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{state};
    return $ret;
}


# Procedure getName
# parameters: 
# - ref - network test refrerence
# Return a test name of network tests from Delphix Engine

sub getName
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getName",1);
    
    my $ret;
    my $network;
        
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } elsif (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
    } else {
      return 'N/A';
    }
    
    $ret = $network->{name};
    return $ret;
}


# Procedure getHost
# parameters: 
# - ref - network test refrerence
# Return a host refrence of network tests from Delphix Engine

sub getHost
{
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Network_obj::getHost",1);
    
    my $ret;
    
    my $network;
    

    
    if (defined($self->{_network_latency}->{$reference})) {
      $network = $self->{_network_latency}->{$reference};
    } elsif (defined($self->{_network_throughput}->{$reference})) {
      $network = $self->{_network_throughput}->{$reference};
   } elsif (defined($self->{_network_dsp}->{$reference})) {
      $network = $self->{_network_dsp}->{$reference};
    } else {
      return undef;
    }
    
    $ret = $network->{parameters}->{remoteHost};
    return $ret;
}

# Procedure getLatencyTestsList
# parameters: 
# - hostref - host 
# Return a list of network latency tests from Delphix Engine
# limit to host if defined

sub getLatencyTestsList
{
   my $self = shift;
   my $hostref = shift;
   logger($self->{_debug}, "Entering Network_obj::getLatencyTestsList",1);

   my @retarr; 
   my $net = $self->{_network_latency};
    
   if (defined($hostref)) {
      @retarr = grep { $net->{$_}->{parameters}->{remoteHost} eq $hostref } sort (keys %{$net});
   } else {
      @retarr = sort { Toolkit_helpers::sort_by_number($a,$b) } (keys %{$net});
   }
 
   return \@retarr;
}

# Procedure getThroughputTestsList
# parameters: 
# - hostref - host
# Return a list of network latency tests from Delphix Engine

sub getThroughputTestsList
{
   my $self = shift;
   my $hostref = shift;
   logger($self->{_debug}, "Entering Network_obj::getThroughputTestsList",1);

   my $net = $self->{_network_throughput};
   my @retarr;

   if (defined($hostref)) {
      @retarr = grep { $net->{$_}->{parameters}->{remoteHost} eq $hostref } sort (keys %{$net});
   } else {
      @retarr = sort { Toolkit_helpers::sort_by_number($a,$b) } (keys %{$net});
   } 


   return \@retarr;
}

# Procedure getDSPTestsList
# parameters: 
# - hostref - host
# Return a list of network DSP tests from Delphix Engine

sub getDSPTestsList
{
   my $self = shift;
   my $hostref = shift;
   logger($self->{_debug}, "Entering Network_obj::getDSPTestsList",1);

   my $net = $self->{_network_dsp};
   my @retarr;
   
   if (defined($hostref)) {
      @retarr = grep { $net->{$_}->{parameters}->{remoteHost} eq $hostref } sort (keys %{$net});
   } else {
      @retarr = sort { Toolkit_helpers::sort_by_number($a,$b) } (keys %{$net});
   } 


   return \@retarr;
}

# Procedure getLatencyLastTests
# parameters: 
# - hostref
# Return a list two last network throughput tests for host from Delphix Engine

sub getLatencyLastTests
{
    my $self = shift;
    my $hostref = shift;
    logger($self->{_debug}, "Entering Network_obj::getLatencyLastTests",1);
    my $net = $self->{_network_throughput};
    # filter only tests for one host
    my $arr = $self->getLatencyTestsList($hostref);
    
    my @retarr;
    
    my $ref = $arr->[-1];
    push (@retarr , $ref);
        
    return \@retarr;
}

# Procedure getDSPLastTests
# parameters: 
# - hostref
# Return a list two last network DSP tests for host from Delphix Engine

sub getDSPLastTests
{
    my $self = shift;
    my $hostref = shift;
    logger($self->{_debug}, "Entering Network_obj::getDSPLastTests",1);
    my $net = $self->{_network_dsp};
    # filter only tests for one host
    my $arr = $self->getDSPTestsList($hostref);
    
    my @retarr;
    
    # take last TRANSMIT 
    my $ref = ( grep { $self->getTestDirection($_) eq 'TRANSMIT' } @{$arr} ) [-1];
    push (@retarr , $ref);
    
    # take last RECEIVE 
    $ref = ( grep { $self->getTestDirection($_) eq 'RECEIVE' } @{$arr} ) [-1];
    push (@retarr , $ref);
    
    return \@retarr;
}

# Procedure getThroughputLastTests
# parameters: 
# - hostref
# Return a list two last network throughput tests for host from Delphix Engine

sub getThroughputLastTests
{
    my $self = shift;
    my $hostref = shift;
    logger($self->{_debug}, "Entering Network_obj::getThroughputTestsList",1);
    my $net = $self->{_network_throughput};
    # filter only tests for one host
    my $arr = $self->getThroughputTestsList($hostref);
    
    my @retarr;
    
    # take last TRANSMIT 
    my $ref = ( grep { $self->getTestDirection($_) eq 'TRANSMIT' } @{$arr} ) [-1];
    push (@retarr , $ref);
    
    # take last RECEIVE 
    $ref = ( grep { $self->getTestDirection($_) eq 'RECEIVE' } @{$arr} ) [-1];
    push (@retarr , $ref);
    
    return \@retarr;
}


# Procedure loadLatencyTest
# parameters: none
# Load a list of network latency tests from Delphix Engine

sub loadLatencyTest 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Network_obj::loadLatencyTest",1);
    my $operation = "resources/json/delphix/network/test/latency";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $network_latency = $self->{_network_latency};

        for my $netitem (@res) {
            $network_latency->{$netitem->{reference}} = $netitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}

# Procedure loadThroughputTest
# parameters: none
# Load a list of network latency tests from Delphix Engine

sub loadThroughputTest 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Network_obj::loadThroughputTest",1);
    my $operation = "resources/json/delphix/network/test/throughput";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $network_thoughput = $self->{_network_throughput};

        for my $netitem (@res) {
            $network_thoughput->{$netitem->{reference}} = $netitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}

# Procedure loadDSPTest
# parameters: none
# Load a list of network DSP tests from Delphix Engine

sub loadDSPTest 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Network_obj::loadDSPTest",1);
    my $operation = "resources/json/delphix/network/test/dsp";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $network_dsp = $self->{_network_dsp};
        
        for my $netitem (@res) {
            $network_dsp->{$netitem->{reference}} = $netitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
    
}

# Procedure runThroughputTest
# parameters: 
# hostref - host reference
# direction - test direction
# numconn - number of connections
# duration - duration
# Load a list of network latency tests from Delphix Engine


sub runThroughputTest 
{
   my $self = shift;
   my $hostref = shift;
   my $direction = shift;
   my $numconn = shift;
   my $duration = shift;
   logger($self->{_debug}, "Entering Network_obj::runThroughputTest",1);

   my $operation = "resources/json/delphix/network/test/throughput";

   if (!defined($numconn)) {
      $numconn = 1;
   }
   
   if (!defined($duration)) {
      $duration = 60;
   }
   
   if (!defined($direction)) {
      $direction = 'TRANSMIT';
   }

   my %testhash = 
   (
       "type" => "NetworkThroughputTestParameters",
       "remoteHost" =>  $hostref,
       "duration" => $duration + 0,
       "numConnections" => $numconn + 0,
       "direction" => $direction
   );
   
   
   my $json_data = to_json(\%testhash);
   
   return $self->runJobOperation($operation, $json_data);
    
}

# Procedure runLatencyTest
# parameters: 
# hostref - host reference
# size - packege size
# count - number of packages
# Load a list of network latency tests from Delphix Engine

sub runLatencyTest 
{
   my $self = shift;
   my $hostref = shift;
   my $size = shift;
   my $count = shift;
   logger($self->{_debug}, "Entering Network_obj::runLatencyTest",1);

   my $operation = "resources/json/delphix/network/test/latency";

   if (!defined($size)) {
      $size = 8192;
   }
   
   if (!defined($count)) {
      $count = 60;
   }

   my %testhash = 
   (
       "type" => "NetworkLatencyTestParameters",
       "remoteHost" =>  $hostref,
       "requestSize" => $size + 0,
       "requestCount" => $count + 0 
   );
   
   
   my $json_data = to_json(\%testhash);
   
   return $self->runJobOperation($operation, $json_data);
    
}



# Procedure runDSPTest
# parameters: 
# hostref - host reference
# size - packege size
# count - number of packages
# Load a list of network latency tests from Delphix Engine

sub runDSPTest 
{
   my $self = shift;
   my $hostref = shift;
   my $direction = shift;
   my $numconn = shift;
   my $duration = shift;
   logger($self->{_debug}, "Entering Network_obj::runDSPTest",1);

   my $operation = "resources/json/delphix/network/test/dsp";

   if (!defined($numconn)) {
      $numconn = 1;
   }
   
   if (!defined($duration)) {
      $duration = 60;
   }
   
   if (!defined($direction)) {
      $direction = 'TRANSMIT';
   }

   my %testhash = 
   (
       "type" => "NetworkDSPTestParameters",
       "remoteHost" =>  $hostref,
       "direction" => $direction,
       "duration" => $duration + 0,
       "numConnections" => $numconn + 0
   );
   
   
   my $json_data = to_json(\%testhash);
   
   return $self->runJobOperation($operation, $json_data);
    
}


# Procedure runJobOperation
# parameters: 
# - operation - API string
# - json_data - JSON encoded data
# Run POST command running background job for particular operation and json data
# Return job number if job started or undef otherwise

sub runJobOperation {
    my $self = shift;
    my $operation = shift;
    my $json_data = shift;
    my $action = shift;

    logger($self->{_debug}, "Entering Network_obj::runJobOperation",1);
    logger($self->{_debug}, $operation, 2);
    
    my ($result, $result_fmt) = $self->{_dlpxObject}->postJSONData($operation, $json_data);
    my $jobno;

    if ( defined($result->{status}) && ($result->{status} eq 'OK' )) {
        if (defined($action) && $action eq 'ACTION') {
            $jobno = $result->{action};
        } else {
            $jobno = $result->{job};
        }
    } else {
        if (defined($result->{error})) {
            print "Problem with job " . $result->{error}->{details} . "\n";
            logger($self->{_debug}, "Can't submit job for operation $operation",1);
            logger($self->{_debug}, $result->{error}->{action} ,1);
        } else {
            print "Unknown error. Try with debug flag\n";
        }
    }

    return $jobno;
}



1;