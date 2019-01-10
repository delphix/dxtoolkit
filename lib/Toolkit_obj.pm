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
# Copyright (c) 2019 by Delphix. All rights reserved.
#
# Program Name : Toolkit_obj.pm
# Description  : Delphix Engine toolkit object
# Author       : Marcin Przepiorowski
# Created      : 08 Jan 2019 (v2.3.0)


package Toolkit_obj;

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
    logger($debug, "Entering Toolkit_obj.pm::constructor",1);

    my %toolkit;
    my $self = {
        _toolkit => \%toolkit,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->LoadToolkit();
    return $self;
}

# Procedure getName
# parameter:
# - ref
# return name of toolkit

sub getName
{
    my $self = shift;
    my $ref = shift;
    logger($self->{_debug}, "Entering Toolkit_obj::getName",1);
    if (defined($self->{_toolkit}->{$ref})) {
      return $self->{_toolkit}->{$ref};
    } else {
      return undef;
    }

}


# Procedure LoadToolkit
# parameters: none
# Load a list of toolkit objects

sub LoadToolkit
{
    my $self = shift;
    logger($self->{_debug}, "Entering Toolkit_obj::LoadToolkit",1);
    my $operation = "resources/json/delphix/toolkit";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
      my @res = @{$result->{result}};

      my $toolkits = $self->{_toolkit};

      for my $toolkititem (@res) {
        $toolkits->{$toolkititem->{reference}} = $toolkititem->{name};
      }

      $self->{_toolkit} = $toolkits;

    } else {
      print "No data returned for $operation. Try to increase timeout \n";
    }

}


1;
