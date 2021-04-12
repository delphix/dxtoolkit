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
# Copyright (c) 2015,2018 by Delphix. All rights reserved.
#
# Program Name : Hook_obj.pm
# Description  : Delphix Engine Hooks
# Author       : Marcin Przepiorowski
# Created      : 16 Feb 2018 (v2.3.X)
#


package Hook_obj;

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
    my $dbonly = shift;
    my $debug = shift;
    logger($debug, "Entering Hook_obj::constructor",1);

    my %hooks_templates;
    my $self = {
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    if (!defined($dbonly)) {
      $self->loadHooksList($debug);
    }
    return $self;
}




# Procedure getType
# parameters:
# - reference
# Return hook type for specific hook template reference

sub getType {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Hook_obj::getType",1);

    my $hooks = $self->{_hooks_templates};
    my $type = $hooks->{$reference}->{operation}->{type};
    my $ret;

    if ($type eq 'RunBashOnSourceOperation') {
        $ret = 'BASH';
    } elsif ($type eq 'RunPowerShellOnSourceOperation') {
        $ret = 'PS';
    } elsif ($type eq 'RunDefaultPowerShellOnSourceOperation') {
        $ret = 'PSD';
    } elsif ($type eq 'RunExpectOnSourceOperation') {
        $ret = 'EXPECT';
    } elsif ($type eq 'RunCommandOnSourceOperation') {
        $ret = 'SHELL';
    } else {
        $ret = $type;
    }

    return $ret;
}

# Procedure getCommand
# parameters:
# - reference
# Return hook command for specific hook template reference

sub getCommand {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Hook_obj::getCommand",1);

    my $hooks = $self->{_hooks_templates};
    my $ret = $hooks->{$reference}->{operation}->{command};
    my $type = $hooks->{$reference}->{operation}->{type};
    if ($type eq 'RunPowerShellOnSourceOperation') {
      $ret =~ s/\r\n/<cr>/g;
    } else {
      $ret =~ s/\n/<cr>/g;
    }
    return $ret;
}



# Procedure exportDBHooks
# parameters:
# - database object
# - location - directory
# Return 0 if no errors

sub exportDBHooks {
    my $self = shift;
    my $dbobj = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Hook_obj::exportDBHooks",1);


    my $hooks = $dbobj->{source}->{operations};

    if (defined($hooks)) {
      my $dbname = $dbobj->getName();
      my $filename =  $location . "/" . $dbname . ".dbhooks";
      print "Exporting database $dbname hooks into  $filename \n";
      $self->exportHook($hooks, $filename);
    }

    return 0;
}


# Procedure importDBHooks
# parameters:
# - database object
# - filename - filename
# Return 0 if no errors

sub importDBHooks {
    my $self = shift;
    my $dbobj = shift;
    my $filename = shift;

    logger($self->{_debug}, "Entering Hook_obj::importDBHooks",1);

    my $hooks = $dbobj->{source}->{operations};
    my $source = $dbobj->{source}->{reference};
    my $type = $dbobj->{source}->{type};
    my $dbname = $dbobj->getName();

    my $loadedHook;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedHook = $json->decode(<$FD>);

    close $FD;

    print "Importing hooks from $filename into database $dbname \n";

    my $operation = 'resources/json/delphix/source/' . $source;

    #print Dumper $loadedHook;

    my %hooks_hash = (
        type => $type,
        operations => $loadedHook
    );

    #print Dumper %hooks_hash;

    my $json_data = to_json(\%hooks_hash);

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    #print Dumper $result_fmt;

    if ($result->{status} eq 'OK') {
        print "Import completed\n";
        return 0;
    } else {
        return 1;
    }

    return 0;
}


# Procedure exportHook
# parameters:
# - hook - content of hook to export
# - location - filename
# Return 0 if no errors

sub exportHook {
    my $self = shift;
    my $hook = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Hook_obj::exportHook",1);

    open (my $FD, '>', "$location") or die ("Can't open file $location : $!");

    print $FD to_json($hook, {pretty => 1});

    close $FD;

}


# Procedure exportHookScript
# parameters:
# - reference
# - location - directory
# Return 0 if no errors

sub exportHookScript {
    my $self = shift;
    my $reference = shift;
    my $filename = shift;

    logger($self->{_debug}, "Entering Hook_obj::exportHookScript",1);

    my $hooks = $self->{_hooks_templates};

    if (!defined($hooks->{$reference})) {
        print "Can't find hook to export \n";
        return 1;
    }

    open (my $FD, '>', "$filename") or die ("Can't open file $filename : $!");

    print "Exporting template into file $filename \n";

    print $FD $hooks->{$reference}->{operation}->{command};

    close $FD;

    return 0;
}


1;
