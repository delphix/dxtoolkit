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
# Program Name : System_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - System_obj - class which map a Delphix Engine system API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
#



package System_obj;

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
    logger($debug, "Entering System_obj::constructor",1);

    my %system;
    my $self = {
        _system => \%system,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    $self->LoadSystem();
    return $self;
}


# Procedure getSSHPublicKey
# parameters: none
# Return an Engine SSH key

sub getSSHPublicKey 
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getSSHPublicKey",1);
    return $self->{_system}->{sshPublicKey};
}

# Procedure getStorage
# parameters: none
# Return an Engine storage hash (Used, Free, Total, pct used) GB

sub getStorage 
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getStorage",1);
    my %stor = (
        Total => sprintf("%2.2f",$self->{_system}->{storageTotal}/1024/1024/1024),
        Used => sprintf("%2.2f",$self->{_system}->{storageUsed}/1024/1024/1024),
        Free => sprintf("%2.2f",($self->{_system}->{storageTotal} - $self->{_system}->{storageUsed})/1024/1024/1024),
        pctused => sprintf("%2.2f",$self->{_system}->{storageUsed} / $self->{_system}->{storageTotal})
    );
    return \%stor;
}

# Procedure getVersion
# parameters: none
# Return an Engine version

sub getVersion 
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::getVersion",1);
    my $verhash = $self->{_system}->{buildVersion};
    return $verhash->{major} . '.' . $verhash->{minor} . '.' . $verhash->{micro} . '.' . $verhash->{patch};
}

# Procedure getUUID
# parameters: none
# return timezone of Delphix engine

sub getUUID {
   my $self = shift;
   logger($self->{_debug}, "Entering System_obj::getUUID",1);
   my $uuid = $self->{_system}->{uuid};

   return $uuid;

}


# Procedure LoadSystem
# parameters: none
# Load a list of System objects from Delphix Engine

sub LoadSystem 
{
    my $self = shift;
    logger($self->{_debug}, "Entering System_obj::LoadSystem",1);
    my $operation = "resources/json/delphix/system";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);

    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
      $self->{_system} = $result->{result};
    } else {
      print "No data returned for $operation. Try to increase timeout \n";
    }


    
}

1;