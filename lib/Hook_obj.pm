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
# Program Name : Hook_obj.pm
# Description  : Delphix Engine Capacity object
# It's include the following classes:
# - Hook_obj - class which map a Delphix Engine operation API object
# Author       : Marcin Przepiorowski
# Created      : 13 Apr 2015 (v2.0.0)
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
        _hooks_templates => \%hooks_templates,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };
    
    bless($self,$classname);
    
    if (!defined($dbonly)) {
      $self->loadHooksList($debug);
    }
    return $self;
}


# Procedure getHookByName
# parameters: 
# - name 
# Return template reference for particular name

sub getHookByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Hook_obj::getHookByName",1);    
    my $ret;



    for my $hookitem ( sort ( keys %{$self->{_hooks_templates}} ) ) {

        if ( $self->getName($hookitem) eq $name) {
            $ret = $hookitem; 
        }
    }

    return $ret;
}

# Procedure getHook
# parameters: 
# - reference
# Return hook template hash for specific hook template reference

sub getHook {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Hook_obj::getHook",1);    

    my $hooks = $self->{_hooks_templates};
    return defined ($hooks->{$reference}) ? $hooks->{$reference} : undef;
}


# Procedure getHookList
# parameters: 
# Return hook template list

sub getHookList {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Hook_obj::getHookList",1);    
    return keys %{$self->{_hooks_templates}};
}


# Procedure getName
# parameters: 
# - reference
# Return hook template name for specific hook template reference

sub getName {
    my $self = shift;
    my $reference = shift;
    
    logger($self->{_debug}, "Entering Hook_obj::getName",1);   

    my $hooks = $self->{_hooks_templates};
    return $hooks->{$reference}->{name};
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


# Procedure exportHookTemplate
# parameters: 
# - reference
# - location - directory
# Return 0 if no errors

sub exportHookTemplate {
    my $self = shift;
    my $reference = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Hook_obj::exportHookTemplate",1);   

    my $hooks = $self->{_hooks_templates};


    if (!defined($hooks->{$reference})) {
        print "Can't find hook to export \n";
        return 1;
    }

    my $hookname = $self->getName($reference);

    if (!defined($hookname)) {
        print "Can't export operation template $hookname \n";
        return 1;
    }

    my $filename =  $location . "/" . $hookname . ".opertemp";

    print "Exporting operation template $hookname into $filename \n";

    $self->exportHook($hooks->{$reference}, $filename);

    return 0;
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





# Procedure importHookTemplate
# parameters: 
# - location - file name
# Return 0 if no errors

sub importHookTemplate {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Hook_obj::importHookTemplate",1);   

    my $filename =  $location;

    my $loadedHook;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedHook = $json->decode(<$FD>);
    
    close $FD;



    delete $loadedHook->{reference};
    delete $loadedHook->{namespace};
    delete $loadedHook->{lastUpdated};

    $self->loadHooksList();

    if (defined($self->getHookByName($loadedHook->{name}))) {
        print "Operation template " . $loadedHook->{name} . " from file $filename already exist.\n";
        return 0;
    }

    print "Importing operation template from file $filename.";

    my $json_data = to_json($loadedHook);

    my $operation = 'resources/json/delphix/source/operationTemplate';

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Import completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure updateHook
# parameters: 
# - location - file name
# Return 0 if no errors

sub updateHookTemplate {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Hook_obj::updateHookTemplate",1);   

    my $filename =  $location;

    my $loadedHook;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedHook = $json->decode(<$FD>);
    
    close $FD;

    delete $loadedHook->{reference};
    delete $loadedHook->{namespace};
    delete $loadedHook->{lastUpdated};

    $self->loadHooksList();

    if (! defined($self->getHookByName($loadedHook->{name}))) {
        print "Operation template " . $loadedHook->{name} . " doesn't exist. Can't update\n";
        return 1;
    }

    my $reference = $self->getHookByName($loadedHook->{name});

    print "Updating operation template " . $loadedHook->{name} . " from file $filename.";

    my $json_data = to_json($loadedHook);

    my $operation = 'resources/json/delphix/source/operationTemplate/' . $reference;

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Update completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure updateHookScript
# parameters: 
# - hook ref
# - location - file name
# Return 0 if no errors

sub updateHookScript {
    my $self = shift;
    my $reference = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Hook_obj::updateHookScript",1);   

    my $filename =  $location;


    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");
    my @script = <$FD>;
    close($FD);  
    my $oneline = join('', @script);

    $self->loadHooksList();

    my $loadedHook = $self->getHook($reference);

    if (!defined($loadedHook)) {
        print "Can't find hook to update \n";
        return 1;
    }

    delete $loadedHook->{reference};
    delete $loadedHook->{namespace};
    delete $loadedHook->{lastUpdated};

    $loadedHook->{operation}->{command} = $oneline;

    print "Updating operation template " . $loadedHook->{name} . " command from file $filename.";

    my $json_data = to_json($loadedHook);

    my $operation = 'resources/json/delphix/source/operationTemplate/' . $reference;

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);  

    if ($result->{status} eq 'OK') {
        print " Update completed\n";
        return 0;
    } else {
        return 1;
    }

}

# Procedure loadHooksList
# parameters: none
# Load a list of hooks objects from Delphix Engine

sub loadHooksList 
{
    my $self = shift;
    logger($self->{_debug}, "Entering Hook_obj::loadHooksList",1);   

    my $operation = "resources/json/delphix/source/operationTemplate";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);


    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};

        my $hooks = $self->{_hooks_templates};

        for my $hookitem (@res) {
            $hooks->{$hookitem->{reference}} = $hookitem;
        } 
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }

}

1;