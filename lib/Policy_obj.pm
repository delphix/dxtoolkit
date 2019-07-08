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
# Program Name : Policy_obj.pm
# Description  : Delphix Engine Policy object
# It's include the following classes:
# - Template_obj - class which map a Delphix Engine policy API object
# Author       : Marcin Przepiorowski
# Created      : 13 Sept 2015 (v2.0.0)
#


package Policy_obj;

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
    logger($debug, "Entering Policy_obj::constructor",1);

    my %policies;
    my $self = {
        _policies => \%policies,
        _dlpxObject => $dlpxObject,
        _debug => $debug
    };

    bless($self,$classname);

    $self->loadPolicyList($debug);
    return $self;
}




# Procedure getName
# parameters:
# - reference
# Return template name for specific template reference

sub getName {
    my $self = shift;
    my $reference = shift;
    my $isInherited = shift;

    logger($self->{_debug}, "Entering Policy_obj::getName",1);

    my $policies = $self->{_policies};

    my $ret;

    if ($reference ne 'N/A') {
        if ($self->getCustomized($reference)) {
            $ret = 'Customized';
        } else {
            $ret = $policies->{$reference}->{name};
            if ($ret =~ /None\:/ ) {
                $ret = 'None';
            }
        }
    } else {
        $ret = 'N/A';
    }

    if ($isInherited) {
        $ret = "* " . $ret;
    }

    return  $ret;
}


# Procedure getSchedule
# parameters:
# - reference
# Return schedule / retention as a single line

sub getSchedule {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering Policy_obj::getSchedule",1);

    my $policy = $self->{_policies}->{$reference};

    my $ret;

    if (defined($policy)) {

        if ($policy->{type} eq 'RetentionPolicy') {

            $ret = "Logs " . $policy->{logDuration} . " " . lc $policy->{logUnit} . "(s), Snapshots " . $policy->{dataDuration} . " " . lc $policy->{dataUnit} . "(s)" ;

            if ($policy->{numOfDaily}) {
                $ret = $ret . ", no of Daily " . $policy->{numOfDaily};
            }

            if ($policy->{numOfWeekly}) {
                $ret = $ret . ", no of Weekly " . $policy->{numOfWeekly} . ", day of week " . $policy->{dayOfWeek};
            }

            if ($policy->{numOfMonthly}) {
                $ret = $ret . ", no of Monthly " . $policy->{numOfMonthly} . ", day of month " . $policy->{dayOfMonth};
            }

            if ($policy->{numOfYearly}) {
                $ret = $ret . ", no of yearly " . $policy->{numOfYearly} . ", day of year " . $policy->{dayOfYear};
            }

        } else {

            if (defined($policy->{scheduleList}) && (scalar(@{$policy->{scheduleList}}) > 0)) {

                my @sorted = sort { (split (' ',$a->{cronString}))[5] <=> (split (' ',$b->{cronString}))[5]  } @{ $policy->{scheduleList} };

                $ret = '';

                for my $scheditem ( @sorted ) {
                    if ($ret eq '') {
                        $ret = Toolkit_helpers::parse_cron($scheditem->{cronString});
                    } else {
                        $ret = $ret . "," . Toolkit_helpers::parse_cron($scheditem->{cronString});
                    }
                }
            } else {
                $ret = 'N/A';
            }

        }
    } else {
        $ret = 'N/A';
    }

    return  $ret;
}


# Procedure getCustomized
# parameters:
# - reference
# Return customized

sub getCustomized {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Policy_obj::getCustomized",1);

    return $self->{_policies}->{$reference}->{customized};
}

# Procedure getType
# parameters:
# - reference
# Return customized

sub getType {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Policy_obj::getType",1);

    return $self->{_policies}->{$reference}->{type};
}


# Procedure getPolicyList
# parameters:
# Return array of policy refernces

sub getPolicyList {
    my $self = shift;
    my $reference = shift;
    logger($self->{_debug}, "Entering Policy_obj::getPolicyList",1);

    return sort ( keys %{$self->{_policies}} );
}

# Procedure isInherited
# parameters:
# - reference
# Return customized

sub isInherited {
    my $self = shift;
    my $reference = shift;
    my $container = shift;

    logger($self->{_debug}, "Entering Policy_obj::isInherited",1);
    my $ret;

    if ($reference ne 'N/A') {
        my $type = $self->{_policies}->{$reference}->{type};



        if ($container =~ /GROUP/ ) {
            $ret = 0;
        } else {
            if (defined($self->{_mapping}->{$container}->{$type}->{inherited})) {
                $ret = $self->{_mapping}->{$container}->{$type}->{inherited} eq 'INHERITED' ? 1 : 0;
            } else {
                $ret = 0;
            }
        }
    } else {
        $ret = 0;
    }
    return $ret;
}

# Procedure getSnapSync
# parameters:
# - container
# - type
# Return SnapSync policy reference for container

sub getSnapSync {
    my $self = shift;
    my $container = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Policy_obj::getSnapSync",1);

    my $ret;

    if ($type eq 'dSource') {
        $ret = $self->{_mapping}->{$container}->{SyncPolicy}->{ref};
    } else {
        $ret = 'N/A';
    }
    return $ret;
}


# Procedure getSnapshot
# parameters:
# - container
# - type
# Return SnapSync policy reference for container

sub getSnapshot {
    my $self = shift;
    my $container = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Policy_obj::getSnapshot",1);

    my $ret;

    if ($type eq 'VDB') {
        $ret = $self->{_mapping}->{$container}->{SnapshotPolicy}->{ref};
    } else {
        $ret = 'N/A';
    }
    return $ret;
}

# Procedure getRetention
# parameters:
# - container
# - type
# Return SnapSync policy reference for container

sub getRetention {
    my $self = shift;
    my $container = shift;
    logger($self->{_debug}, "Entering Policy_obj::getRetention",1);

    my $ret = $self->{_mapping}->{$container}->{RetentionPolicy}->{ref};
    return $ret;
}

# Procedure getRefresh
# parameters:
# - container
# - type
# Return SnapSync policy reference for container

sub getRefresh {
    my $self = shift;
    my $container = shift;
    my $type = shift;
    logger($self->{_debug}, "Entering Policy_obj::getRefresh",1);

    my $ret;

    if ($type eq 'dSource') {
        $ret = 'N/A';
    } else {
        if (defined($self->{_mapping}->{$container}->{RefreshPolicy}->{ref})) {
            $ret = $self->{_mapping}->{$container}->{RefreshPolicy}->{ref};
        } else {
            $ret = $self->getPolicyByName('None:RefreshPolicy');
        }
    }
    return $ret;
}


# Procedure exportPolicy
# parameters:
# - reference
# - location - directory
# Return 0 if no errors

sub exportPolicy {
    my $self = shift;
    my $reference = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Policy_obj::exportPolicy",1);

    my $name = $self->getName($reference);

    if (($name eq 'Customized') || ($name eq 'None') || ($name eq 'N/A')) {
        return 1;
    }

    $name =~ s/\//_/g;
    $name =~ s/\\/_/g;

    my $filename =  $location . "/" . $name . ".policy";

    my $polices = $self->{_policies};

    open (my $FD, '>', "$filename") or die ("Can't open file $filename : $!");

    print "Exporting policy into file $filename \n";

    print $FD to_json($polices->{$reference}, {pretty => 1});

    close $FD;

    return 0;
}


# Procedure exportMapping
# parameters:
# - location - directory
# Return 0 if no errors

sub exportMapping {
    my $self = shift;
    my $filename = shift;
    my $groups = shift;
    my $databases = shift;

    logger($self->{_debug}, "Entering Policy_obj::exportMapping",1);

    my %exportmapping;

    for my $contitem ( grep { $_ =~ /GROUP/ } keys %{$self->{_mapping}} ) {
        my $contname = $groups->getName($contitem);

        for my $policytype ( keys %{$self->{_mapping}->{$contitem} }) {
            if ($self->getName($self->{_mapping}->{$contitem}->{$policytype}->{ref}) eq 'Customized') {
                next;
            }
            $exportmapping{$contname}{$policytype}{name} = $self->getName($self->{_mapping}->{$contitem}->{$policytype}->{ref});
            $exportmapping{$contname}{$policytype}{inherited}  = $self->{_mapping}->{$contitem}->{$policytype}->{inherited};
        };
    }


    for my $contitem ( grep { ! ($_ =~ /GROUP/) } keys %{$self->{_mapping}} ) {
        my $contname = $databases->getDB($contitem)->getName();
        my $groupname = $groups->getName($databases->getDB($contitem)->getGroup());

        for my $policytype ( keys %{$self->{_mapping}->{$contitem}} ) {
            if ($self->getName($self->{_mapping}->{$contitem}->{$policytype}->{ref}) eq 'Customized') {
                next;
            }
            $exportmapping{$groupname}{databases}{$contname}{$policytype}{name} = $self->getName($self->{_mapping}->{$contitem}->{$policytype}->{ref});
            $exportmapping{$groupname}{databases}{$contname}{$policytype}{inherited}  = $self->{_mapping}->{$contitem}->{$policytype}->{inherited};
        };

    }


    open (my $FD, '>', "$filename") or die ("Can't open file $filename : $!");

    print "Exporting mapping into file $filename \n";

    print $FD to_json(\%exportmapping, {pretty => 1});

    close $FD;

    return 0;
}

# Procedure applyMapping
# parameters:
# - location - file
# Return 0 if no errors

sub applyMapping {
    my $self = shift;
    my $filename = shift;
    my $groups = shift;
    my $databases = shift;

    logger($self->{_debug}, "Entering Policy_obj::applyMapping",1);

    my $loadedMapping;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedMapping = $json->decode(<$FD>);

    close $FD;

    for my $groupname ( keys %{$loadedMapping} ) {
        my $contref;

        my $groupref = $groups->getGroupByName($groupname);

        for my $policytype ( grep { ! ($_ eq 'databases') } keys %{$loadedMapping->{$groupname} } ) {

            my $policyref = $self->getPolicyByName($loadedMapping->{$groupname}->{$policytype}->{name});

            if (! defined($policyref) ) {
                next;
            }

            if ($loadedMapping->{$groupname}->{$policytype}->{inherited} eq 'DIRECT_APPLIED' ) {
                print "Applying policy " . $loadedMapping->{$groupname}->{$policytype}->{name} . " to group " . $groupname ;
                if ($self->applyPolicy($policyref, $groupref)) {
                    return 1;
                };
            }

        };

        for my $dbname ( keys %{$loadedMapping->{$groupname}->{databases} } ) {
            my $contref;

            for my $policytype ( keys %{$loadedMapping->{$groupname}->{databases}->{$dbname} } ) {

                my $policyref = $self->getPolicyByName($loadedMapping->{$groupname}->{databases}->{$dbname}->{$policytype}->{name});

                if (! defined($policyref) ) {
                    next;
                }

                my $db = Toolkit_helpers::get_dblist_from_filter(undef,$groupname,undef,$dbname,$databases,$groups,undef);

                if (! defined($db)) {
                    print "Database $dbname in group $groupname doesn't exist. Skipping\n";
                    next;
                }

                if (scalar(@{$db}) > 1) {
                    print "More than one db selected by group and db name\n";
                    return 1;
                }

                my $dbref = $db->[0];

                if ($loadedMapping->{$groupname}->{databases}->{$dbname}->{$policytype}->{inherited} eq 'DIRECT_APPLIED' ) {
                    print "Applying policy " . $loadedMapping->{$groupname}->{databases}->{$dbname}->{$policytype}->{name} . " to database " . $dbname . " ";
                    if ($self->applyPolicy($policyref, $dbref)) {
                        return 1;
                    };
                }

            };

        }

    }


    return 0;
}


# Procedure applyPolicy
# parameters:
# - ref
# - target
# Return 0 if no errors

sub applyPolicy {
    my $self = shift;
    my $ref = shift;
    my $target = shift;

    logger($self->{_debug}, "Entering Policy_obj::applyPolicy",1);

    my %applypolicy = (
        type => 'PolicyApplyTargetParameters',
        target => $target
    );

    my $json_data = to_json(\%applypolicy);

    my $operation = 'resources/json/delphix/policy/' . $ref . '/apply';

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ($result->{status} eq 'OK') {
        print "Apply completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure importPolicy
# parameters:
# - location - file name
# Return 0 if no errors

sub importPolicy {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Policy_obj::importPolicy",1);

    my $filename =  $location;

    my $loadedPolicy;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedPolicy = $json->decode(<$FD>);

    close $FD;


    delete $loadedPolicy->{reference};
    delete $loadedPolicy->{namespace};
    delete $loadedPolicy->{default};
    delete $loadedPolicy->{effectiveType};
    delete $loadedPolicy->{timezone}->{offset};

    $self->loadPolicyList();

    if (defined($self->getPolicyByName($loadedPolicy->{name}))) {
        print "Policy " . $loadedPolicy->{name} . " from file $filename already exist.\n";
        return 1;
    }

    print "Importing policy from file $filename.";

    my $json_data = to_json($loadedPolicy);

    my $operation = 'resources/json/delphix/policy';

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ($result->{status} eq 'OK') {
        print " Import completed\n";
        return 0;
    } else {
        return 1;
    }

}


# Procedure updatePolicy
# parameters:
# - location - file name
# Return 0 if no errors

sub updatePolicy {
    my $self = shift;
    my $location = shift;

    logger($self->{_debug}, "Entering Policy_obj::updatePolicy",1);

    my $filename =  $location;

    my $loadedPolicy;

    open (my $FD, '<', "$filename") or die ("Can't open file $filename : $!");

    local $/ = undef;
    my $json = JSON->new();
    $loadedPolicy = $json->decode(<$FD>);

    close $FD;

    delete $loadedPolicy->{reference};
    delete $loadedPolicy->{namespace};
    delete $loadedPolicy->{default};
    delete $loadedPolicy->{effectiveType};
    delete $loadedPolicy->{customized};
    delete $loadedPolicy->{timezone}->{offset};

    if (defined($loadedPolicy->{timezone}->{offsetString})) {
      delete $loadedPolicy->{timezone}->{offsetString};
    }

    $self->loadPolicyList();

    if (! defined($self->getPolicyByName($loadedPolicy->{name}))) {
        print "Policy " . $loadedPolicy->{name} . " from file $filename doesn't exist. Can't update.\n";
        return 1;
    }

    my $reference = $self->getPolicyByName($loadedPolicy->{name});

    print "Updating policy " . $loadedPolicy->{name} . " from file $filename.";

    my $json_data = to_json($loadedPolicy);

    my $operation = 'resources/json/delphix/policy/' . $reference;

    my ($result, $result_fmt, $retcode) = $self->{_dlpxObject}->postJSONData($operation, $json_data);

    if ($result->{status} eq 'OK') {
        print " Update completed\n";
        return 0;
    } else {
        return 1;
    }

}

# Procedure loadPolicyMapping
# parameters:
# - containers - list of references
# Create a mapping of policy for conteiners

sub loadPolicyMapping
{
    my $self = shift;
    my $conteiners = shift;
    logger($self->{_debug}, "Entering Policy_obj::loadPolicyMapping",1);

    for my $contitem ( @{$conteiners} ) {
        my $operation = "resources/json/delphix/policy?effective=true&target=" . $contitem;
        my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
        if (defined($result->{status}) && ($result->{status} eq 'OK')) {
            my @res = @{$result->{result}};

            for my $policyitem (@res) {

                if ($policyitem->{type} eq 'SyncPolicy') {
                    $self->{_mapping}->{$contitem}->{SyncPolicy}->{ref} = $policyitem->{reference};
                    $self->{_mapping}->{$contitem}->{SyncPolicy}->{inherited} = $policyitem->{effectiveType};
                } elsif ($policyitem->{type} eq 'RetentionPolicy') {
                    $self->{_mapping}->{$contitem}->{RetentionPolicy}->{ref} = $policyitem->{reference};
                    $self->{_mapping}->{$contitem}->{RetentionPolicy}->{inherited} = $policyitem->{effectiveType};
                } elsif ($policyitem->{type} eq 'SnapshotPolicy') {
                    $self->{_mapping}->{$contitem}->{SnapshotPolicy}->{ref} = $policyitem->{reference};
                    $self->{_mapping}->{$contitem}->{SnapshotPolicy}->{inherited} = $policyitem->{effectiveType};
                } elsif ($policyitem->{type} eq 'RefreshPolicy') {
                    $self->{_mapping}->{$contitem}->{RefreshPolicy}->{ref} = $policyitem->{reference};
                    $self->{_mapping}->{$contitem}->{RefreshPolicy}->{inherited} = $policyitem->{effectiveType};
                }
            }
        } else {
            print "No data returned for $operation. Try to increase timeout \n";
        }

    }

}


# Procedure getPolicyByName
# parameters:
# - name
# Return a policy ref for policy name

sub getPolicyByName
{
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering Policy_obj::getPolicyByName",1);

    my $ret;
    my $policies = $self->{_policies};

    for my $policyitem ( keys %{$policies} ) {
        if (defined($policies->{$policyitem}->{name}) && ( $policies->{$policyitem}->{name} eq $name) ) {
            $ret = $policyitem;
        }
    }

    return $ret;
}


# Procedure loadPolicyList
# parameters: none
# Load a list of policies objects from Delphix Engine

sub loadPolicyList
{
    my $self = shift;
    logger($self->{_debug}, "Entering Policy_obj::loadPolicyList",1);

    my $operation = "resources/json/delphix/policy";
    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $policies = $self->{_policies};

        for my $policyitem (@res) {
            $policies->{$policyitem->{reference}} = $policyitem;
        }
    } else {
        print "No data returned for $operation. Try to increase timeout \n";
    }
}

1;
