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
# Program Name : JS_container_obj.pm
# Description  : Delphix Engine JS template
# Author       : Marcin Przepiorowski
# Created      : Apr 2016 (v2.2.4)
#
#


package JS_container_obj;

use warnings;
use strict;
use Data::Dumper;
use JSON;
use Toolkit_helpers qw (logger);
use JS_bookmark_obj;
use JS_datasource_obj;
use Snapshot_obj;
use Date::Manip;

# constructor
# parameters
# - dlpxObject - connection to DE
# - debug - debug flag (debug on if defined)

sub new {
    my $classname  = shift;
    my $dlpxObject = shift;
    my $template_ref = shift;
    my $debug = shift;
    logger($debug, "Entering JS_container_obj::constructor",1);

    my %jscontainer;
    my $self = {
        _jscontainer => \%jscontainer,
        _dlpxObject => $dlpxObject,
        _template_ref => $template_ref,
        _debug => $debug
    };

    bless($self,$classname);

    $self->loadJSContainerList($debug);
    return $self;
}


# Procedure getJSContainerByName
# parameters:
# - name
# Return template reference for particular name

sub getJSContainerByName {
    my $self = shift;
    my $name = shift;
    logger($self->{_debug}, "Entering JS_container_obj::getJSContainerByName",1);
    my $ret;

    for my $containeritem ( sort ( keys %{$self->{_jscontainer}} ) ) {

        if ( $self->getName($containeritem) eq $name) {
            $ret = $containeritem;
        }
    }

    return $ret;
}

# Procedure getJSContainer
# parameters:
# - reference
# Return container hash for specific container reference

sub getJSContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSContainer",1);

    my $container = $self->{_jscontainer};
    return $container->{$reference};
}


# Procedure getJSActiveBranch
# parameters:
# - reference
# Return active branch for container for specific container reference

sub getJSActiveBranch {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSActiveBranch",1);

    my $container = $self->{_jscontainer};
    return $container->{$reference}->{activeBranch};
}



# Procedure getJSContainerTemplate
# parameters:
# - reference
# Return active branch for container for specific container reference

sub getJSContainerTemplate {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSContainerTemplate",1);

    my $container = $self->{_jscontainer};
    return $container->{$reference}->{template};
}


# Procedure getJSContainerList
# parameters:
# Return JS container list

sub getJSContainerList {
    my $self = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSTemplateList",1);

    my @arrret = sort (keys %{$self->{_jscontainer}} );

    return \@arrret;
}

# Procedure getJSFirstOperation
# parameters:
# - reference
# Return firstoperation for container for specific container reference

sub getJSFirstOperation {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getJSFirstOperation",1);

    my $jscontainer = $self->{_jscontainer};
    return $jscontainer->{$reference}->{firstOperation};
}


# Procedure getName
# parameters:
# - reference
# Return JS container name for specific container reference

sub getName {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getName",1);

    my $jscontainer = $self->{_jscontainer};
    return $jscontainer->{$reference} ? $jscontainer->{$reference}->{name} : 'N/A';
}


# Procedure getProperties
# parameters:
# - reference
# Return JS container properties hash for specific container reference

sub getProperties {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_container_obj::getProperties",1);

    my $jscontainer = $self->{_jscontainer};
    return $jscontainer->{$reference}->{properties};
}


# Procedure loadJSContainerList
# parameters: none
# Load a list of container objects from Delphix Engine

sub loadJSContainerList
{
    my $self = shift;
    logger($self->{_debug}, "Entering JS_container_obj::loadJSContainerList",1);

    my $operation = "resources/json/delphix/jetstream/container";

    if (defined($self->{_template_ref})) {
        $operation = $operation . "?template=" . $self->{_template_ref} ;
    }


    my ($result, $result_fmt) = $self->{_dlpxObject}->getJSONResult($operation);
    if (defined($result->{status}) && ($result->{status} eq 'OK')) {
        my @res = @{$result->{result}};
        my $jscontainer = $self->{_jscontainer};

        for my $containeritem (@res) {
            $jscontainer->{$containeritem->{reference}} = $containeritem;
        }
    }
}


# Procedure restoreContainer
# parameters:
# - reference
# - branchref
# - timestamp
# - $dataobj_ref (if restore in 5.0 or lower)
# recover container
# return job reference

sub restoreContainer {
    my $self = shift;
    my $reference = shift;
    my $branchref = shift;
    my $timestamp = shift;
    my $dataobj_ref = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::restoreContainer",1);

    my $detz = $self->{_dlpxObject}->getTimezone();

    my %timelineHash;

    my $zulutime;

    if ( $timestamp =~ /(\d\d\d\d)-(\d\d)-(\d\d) (\d?\d):(\d?\d):(\d\d)/ ) {


        chomp($timestamp);
        $timestamp =~ s/T/ /;
        $timestamp =~ s/\.000Z//;

        my $ret;
        $zulutime = Toolkit_helpers::convert_to_utc($timestamp, $detz, undef, 1);

        if (defined($zulutime)) {

            if ($self->{_dlpxObject}->getApi() lt "1.8") {
              %timelineHash = (
                  "type" => "JSTimelinePointTimeInput",
                  "sourceDataLayout" => $dataobj_ref,
                  "time" => $zulutime
              );
            } else {
              %timelineHash = (
                  "type" => "JSTimelinePointTimeInput",
                  "branch" => $branchref,
                  "time" => $zulutime
              );
            }
        } else {
            print "Can't parse timestamp - $timestamp \n";
            return undef;
        }
    } else {
        # maybe it's bookmark name
        my $bookmarks = new JS_bookmark_obj ( $self->{_dlpxObject}, undef, undef, $self->{_debug} );
        my $bookmark_ref = $bookmarks->getJSBookmarkByName($timestamp);

        if (defined($bookmark_ref)) {
            %timelineHash = (
                "type" => "JSTimelinePointBookmarkInput",
                "bookmark" => $bookmark_ref
            );
        } else {
            print "Timestamp doesn't match a required format nor any bookmark name - $timestamp \n";
            return undef;
        }

    }

    my %recoveryHash;

    if ($self->{_dlpxObject}->getApi() lt "1.9") {
      %recoveryHash = %timelineHash;
    } else {
      %recoveryHash =  (
        "type" => "JSDataContainerRestoreParameters",
        "forceOption" => JSON::false,
        "timelinePointParameters" => \%timelineHash
      );
    }

    my $json_data = to_json(\%recoveryHash);

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/restore";

    return $self->runJobOperation($operation, $json_data);

}


# Procedure refreshContainer
# parameters:
# - reference
# refresh container
# return job reference

sub refreshContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::refreshContainer",1);

    my %refreshHash;

    if ($self->{_dlpxObject}->getApi() lt "1.9") {
      %refreshHash = ();
    } else {
      %refreshHash =  (
        "type" => "JSDataContainerRefreshParameters",
        "forceOption" => JSON::false
      );
    }

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/refresh";
    my $json_data = to_json(\%refreshHash);

    return $self->runJobOperation($operation, $json_data);

}



# Procedure resetContainer
# parameters:
# - reference
# reset container
# return job reference

sub resetContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::resetContainer",1);

    my %resetHash;

    if ($self->{_dlpxObject}->getApi() lt "1.9") {
      %resetHash = ();
    } else {
      %resetHash =  (
        "type" => "JSDataContainerResetParameters",
        "forceOption" => JSON::false
      );
    }

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/reset";
    my $json_data = to_json(\%resetHash);

    return $self->runJobOperation($operation, $json_data);

}

# Procedure enableContainer
# parameters:
# - reference
# enable container
# return job reference

sub enableContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::enableContainer",1);

    if ($self->{_jscontainer}->{$reference}->{state} eq "ONLINE") {
      print "Container is already online\n";
      return undef;
    } else {
      my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/enable";
      return $self->runJobOperation($operation, '{}');
    }

}


# Procedure disableContainer
# parameters:
# - reference
# disable container
# return job reference

sub disableContainer {
    my $self = shift;
    my $reference = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::disableContainer",1);

    if ($self->{_jscontainer}->{$reference}->{state} eq "OFFLINE") {
      print "Container is already offline\n";
      return undef;
    } else {
      my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/disable";
      return $self->runJobOperation($operation, '{}');
    }
}

# Procedure addOwner
# parameters:
# - reference
# - user ref
# add owner to container
# return job reference

sub addOwner {
    my $self = shift;
    my $reference = shift;
    my $userref = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::addOwner",1);
    my %addownhash = (
      "type" => "JSDataContainerModifyOwnerParameters",
      "owner" => $userref
    );

    my $json_date = to_json(\%addownhash);

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/addOwner";
    return $self->runJobOperation($operation, $json_date, 'ACTION');

}

# Procedure removeOwner
# parameters:
# - reference
# - user ref
# remove owner from container
# return job reference

sub removeOwner {
    my $self = shift;
    my $reference = shift;
    my $userref = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::removeOwner",1);
    my %addownhash = (
      "type" => "JSDataContainerModifyOwnerParameters",
      "owner" => $userref
    );

    my $json_date = to_json(\%addownhash);

    my $operation = "resources/json/delphix/jetstream/container/" . $reference . "/removeOwner";
    return $self->runJobOperation($operation, $json_date, 'ACTION');

}

# Procedure createContainer
# parameters:
# - container name
# - template reference
# - container def
# - owner array
# create container
# return job reference

sub createContainer {
    my $self = shift;
    my $name = shift;
    my $template_ref = shift;
    my $container_def = shift;
    my $owners_array = shift;
    my $dontrefresh = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::createContainer",1);

    my $operation = "resources/json/delphix/jetstream/container";


    my @datasources;

    for my $contitem (@{$container_def}) {
      my %conthashitem = (
        "type" => "JSDataSourceCreateParameters",
        "source" => {
            "type" => "JSDataSource",
            "priority" => 1,
            "name" => $contitem->{source}
        },
        "container" => $contitem->{vdb_ref}
      );
      push(@datasources, \%conthashitem);

    }

    my %conthash;

    if ($self->{_dlpxObject}->getApi() lt "1.8.2") {
      if (defined($dontrefresh)) {
        print "Your Delphix Engine version doesn't allow JS container creation without refresh.\n";
        return undef;
      }
      %conthash = (
        "type" => "JSDataContainerCreateParameters",
        "dataSources" => \@datasources,
        "name" => $name,
        "template" => $template_ref,
        "owners" => $owners_array,
        "timelinePointParameters" => {
            "type" => "JSTimelinePointLatestTimeInput",
            "sourceDataLayout" => $template_ref
        }
      );
    } else {
      if (defined($dontrefresh)) {
        %conthash = (
          "type" => "JSDataContainerCreateWithoutRefreshParameters",
          "dataSources" => \@datasources,
          "name" => $name,
          "template" => $template_ref,
          "owners" => $owners_array
        );
      } else {
        %conthash = (
          "type" => "JSDataContainerCreateWithRefreshParameters",
          "dataSources" => \@datasources,
          "name" => $name,
          "template" => $template_ref,
          "owners" => $owners_array,
          "timelinePointParameters" => {
              "type" => "JSTimelinePointLatestTimeInput",
              "sourceDataLayout" => $template_ref
          }
        );
      }
    }

    my $json_data = to_json(\%conthash);
    return $self->runJobOperation($operation, $json_data);

}

# Procedure deleteContainer
# parameters:
# - container ref
# - dropvdb
# drop container
# return job reference

sub deleteContainer {
    my $self = shift;
    my $container_ref = shift;
    my $dropvdb = shift;

    logger($self->{_debug}, "Entering JS_bookmark_obj::deleteContainer",1);

    my $operation = "resources/json/delphix/jetstream/container/" . $container_ref . "/delete";

    my $drop;

    if ($dropvdb eq 'yes') {
      $drop = JSON::true
    } else {
      $drop = JSON::false
    }

    my %dropcont = (
        "type" => "JSDataContainerDeleteParameters",
        "deleteDataSources" => $drop
    );

    my $json_data = to_json(\%dropcont);
    return $self->runJobOperation($operation, $json_data);

}

#     "timelinePointParameters": {
#         "type": "JSTimelinePointTimeInput",
#         "branch": "JS_BRANCH-26",
#         "time": "2017-03-18T00:00:00.000Z"
#     }
# }


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

    logger($self->{_debug}, "Entering JS_bookmark_obj::runJobOperation",1);
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
