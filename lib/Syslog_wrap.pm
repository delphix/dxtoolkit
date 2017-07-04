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
# Copyright (c) 2017 by Delphix. All rights reserved.
#



package Syslog_wrap;

use warnings;
use strict;
use Data::Dumper;
use Log::Syslog::Fast ':all';
use Log::Syslog::Constants;
use Toolkit_helpers qw (logger);
use Try::Tiny;



# constructor
# parameters 
# - server - syslog server
# - port - syslog port
# - protocol - connection protocol TCP/UDP
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $server = shift;
    my $port = shift;
    my $protocol = shift;
    my $debug = shift;
    logger($debug, "Entering Syslog_wrap::constructor",1);
    
    if (lc $protocol eq 'tcp') {
      $protocol = LOG_TCP;
    } elsif (lc $protocol eq 'udp') {
      $protocol = LOG_UDP;
    } else {
      print "Wrong protocol\n";
      return undef;
    }
    
    my $handler;
    my $self;

    try {
      $handler = Log::Syslog::Fast->new($protocol, $server, $port, Log::Syslog::Constants::LOG_USER, Log::Syslog::Constants::LOG_INFO, "servername", "Delphix");
      $handler->set_format(LOG_RFC5424);
      $handler->set_pid(0);
      $self = {
          _server => $server,
          _port => $port,
          _protocol => $protocol,
          _handler => $handler
      };
      
      bless($self,$classname);
    }
    catch {
         print "Can't connect to syslog server: " . $_ . " \n" ;
    };

    return $self;
}


# procedure set_facility 
# set facility 
# - facility

sub set_facility {
  my $self = shift;
  my $facility_name = shift;
  
  my $facility = Log::Syslog::Constants::get_facility($facility_name);
  
  if ( ! defined($facility) ) {
    print "Facility $facility_name not found\n";
    return 1;
  }
  
  try {
    $self->{_handler}->set_facility($facility);
    return 0;
  }
  catch {
    print "Problem with setting facility" . $_ . " \n" ;
    return 1;   
  };
    
}

# procedure set_severity 
# set severity 
# - severity

sub set_severity {
  my $self = shift;
  my $severity_name = shift;

  my $severity = Log::Syslog::Constants::get_severity($severity_name);
  
  if ( ! defined($severity) ) {
    print "Severity $severity_name not found\n";
    return 1;
  }
  
  try {
    $self->{_handler}->set_severity($severity);
    return 0;
  }
  catch {
    print "Problem with setting severity" . $_ . " \n" ;
    return 1;   
  };
    
}

# procedure setDE 
# set source to Delphix Engine IP
# - address

sub setDE {
  my $self = shift;
  my $address = shift;
    
  $self->{_handler}->set_sender($address);
    
}

# procedure send 
# send message to syslog
# - message

sub send {
  my $self = shift;
  my $message = shift;
  my $time = shift;
  
  if ($self->{_protocol} eq LOG_TCP) {
    $message = $message . "\n";
  }
  
  if (!defined($time)) {
    $time = time;
  }
  

  $self->{_handler}->send($message, $time);
}

1;