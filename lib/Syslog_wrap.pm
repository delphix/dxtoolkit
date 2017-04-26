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

    my $handler = Log::Syslog::Fast->new($protocol, $server, $port, Log::Syslog::Constants::LOG_USER, Log::Syslog::Constants::LOG_INFO, "servername", "Delphix");

    my $self = {
        _server => $server,
        _port => $port,
        _protocol => $protocol,
        _handler => $handler
    };


    
    bless($self,$classname);
    return $self;
}

# procedure send 
# send message to syslog
# - message

sub send {
  my $self = shift;
  my $message = shift;
  
  if ($self->{_protocol} eq LOG_TCP) {
    $message = $message . "\n";
  }
  
  $self->{_handler}->send($message, time);
    
}

1;